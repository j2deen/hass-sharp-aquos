"""Data update coordinator for the Sharp Aquos TV integration.

Polls every field the media_player and sensor platforms need in a single
pass so entities never issue their own redundant TCP round trips.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import logging
from datetime import timedelta

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .aquos import AquosCommandError, AquosConnectionError, AquosReplyError, AquosTV
from .const import DOMAIN, UPDATE_INTERVAL_SECONDS

_LOGGER = logging.getLogger(__name__)


@dataclass
class AquosData:
    """Snapshot of everything we know about the TV right now."""

    is_on: bool
    volume: int | None = None
    is_muted: bool | None = None
    source_code: int | None = None
    aspect_ratio: int | None = None
    audio_selection: int | None = None
    av_mode: int | None = None
    backlight: int | None = None
    channel: int | None = None
    signal_strength: int | None = None
    model: str | None = None


# Everything polled per cycle once the TV is confirmed on. Each entry is
# (AquosData attribute, AquosTV method name, optional "is this reading
# meaningful right now?" predicate). The predicate is checked against the
# partially-filled snapshot, so it can gate on fields read earlier in the
# list - which is why source_code comes before signal_strength.
POLLED_FIELDS: tuple[tuple[str, str, Callable[[AquosData], bool] | None], ...] = (
    ("volume", "volume", None),
    ("is_muted", "mute", None),
    ("source_code", "input_source", None),
    ("aspect_ratio", "aspect_ratio", None),
    ("audio_selection", "audio_selection", None),
    ("av_mode", "av_mode", None),
    ("backlight", "backlight", None),
    # Tuner channel and signal strength only mean anything on the
    # TV/antenna input; on HDMI the TV rejects the queries, which must not
    # be mistaken for "this model doesn't support them".
    ("channel", "channel", lambda d: d.source_code == 0),
    ("signal_strength", "signal_strength", lambda d: d.source_code == 0),
)


# A field is only written off as unsupported after this many consecutive
# ERR replies. One ERR can be state-dependent (the TV was mid-transition,
# or on an input where the query is meaningless); three in a row across
# separate polls is the model saying no.
UNSUPPORTED_AFTER_REJECTIONS = 3


class AquosDataUpdateCoordinator(DataUpdateCoordinator[AquosData]):
    """Polls the TV on an interval and hands entities a shared snapshot."""

    def __init__(self, hass: HomeAssistant, tv: AquosTV, power_on_enabled: bool) -> None:
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=UPDATE_INTERVAL_SECONDS),
        )
        self.tv = tv
        self._power_on_enabled = power_on_enabled
        self._model: str | None = None
        self._warned_fields: set[str] = set()
        # Fields the TV has answered at least once, and fields it has
        # actively rejected. A field in neither set hasn't been probed yet
        # (the TV may not have been on long enough).
        self._supported_fields: set[str] = set()
        self._unsupported_fields: set[str] = set()
        self._rejections: dict[str, int] = {}
        # The TV's "accept power-on over TCP" flag only needs setting once
        # per session, not on every poll - and it is rejected in standby.
        self._power_on_setting_synced = False

    @property
    def supported_fields(self) -> frozenset[str]:
        """Fields this TV has successfully returned a value for."""
        return frozenset(self._supported_fields)

    def is_field_supported(self, field: str) -> bool | None:
        """True/False once the TV has answered or rejected the field, None
        while it's still unknown."""
        if field in self._supported_fields:
            return True
        if field in self._unsupported_fields:
            return False
        return None

    def _log_field_failure(self, field: str, err: Exception) -> None:
        """Log a field read failure, treating the two error types
        differently since they mean different things:

        - AquosConnectionError: the TV just didn't answer this round trip.
          These integrations are commonly talking to 10-15 year old TVs over
          wifi that was never great to begin with, so a connection dropping
          occasionally is expected, ongoing behaviour, not a one-time fact
          worth suppressing after the first occurrence - warn every time.
        - AquosCommandError: the TV answered but rejected/doesn't support
          this specific command. That's a stable fact about this model that
          won't change poll to poll, so it only needs to be surfaced once
          (falling back to debug afterward) or it's just permanent log spam.
        """
        if isinstance(err, AquosConnectionError):
            _LOGGER.warning("Could not read %s: %s", field, err)
            return
        if isinstance(err, AquosReplyError):
            # One garbled exchange; the next poll retries it. Not worth a
            # warning per occurrence on a flaky link.
            _LOGGER.debug("Garbled reply for %s, will retry next poll: %s", field, err)
            return
        if field not in self._warned_fields:
            self._warned_fields.add(field)
            _LOGGER.warning("Could not read %s (won't repeat this warning): %s", field, err)
        else:
            _LOGGER.debug("Could not read %s: %s", field, err)

    async def _async_update_data(self) -> AquosData:
        try:
            is_on = await self.tv.power()
        except AquosConnectionError:
            # Treat unreachable as off rather than failing the whole
            # coordinator - a TV that's fully unplugged/asleep will simply
            # not answer, and that's a legitimate (common) state, not an
            # integration error.
            return AquosData(is_on=False, model=self._model)
        except AquosReplyError as err:
            # The TV answered but not with a clean 0/1. That is one bad
            # exchange, not a state change: keep whatever we last knew
            # rather than flipping on/off for a poll.
            _LOGGER.debug("Power query garbled, keeping last state: %s", err)
            if self.data is not None:
                return self.data
            return AquosData(is_on=False, model=self._model)

        if not is_on:
            return AquosData(is_on=False, model=self._model)

        # Sync the TV's "accept power-on over TCP/IP" flag with the config
        # option once per session, while the TV is on (standby rejects it).
        # Upstream re-sent it on every poll - a wasted round trip.
        if not self._power_on_setting_synced:
            try:
                await self.tv.set_power_on_command_settings(self._power_on_enabled)
            except (AquosConnectionError, AquosCommandError) as err:
                self._log_field_failure("power_on_command_settings", err)
            else:
                self._power_on_setting_synced = True

        if self._model is None:
            try:
                self._model = await self.tv.model()
            except (AquosConnectionError, AquosCommandError) as err:
                self._log_field_failure("model", err)

        data = AquosData(is_on=True, model=self._model)
        # Each of these is its own full TCP round trip (connect, auth, two
        # command steps) and can legitimately take seconds. Look the method
        # up and call it fresh right before awaiting it, rather than
        # building every coroutine object upfront in a tuple literal - if
        # an earlier field's await gets cancelled (a config reload mid-poll,
        # a slow/unresponsive TV blowing past the update interval), any
        # coroutine that was constructed but never reached is silently
        # dropped un-awaited, which is exactly what a "coroutine was never
        # awaited" RuntimeWarning means.
        for attr, method_name, applicable in POLLED_FIELDS:
            if attr in self._unsupported_fields:
                # The TV already told us it doesn't do this one. Each read
                # is a full round trip, so don't keep asking.
                continue
            if applicable is not None and not applicable(data):
                continue
            try:
                setattr(data, attr, await getattr(self.tv, method_name)())
            except (AquosConnectionError, AquosReplyError) as err:
                # Transient - a dropped connection or a garbled reply says
                # nothing about whether the field is supported. Leave it
                # None this round and try again next.
                self._log_field_failure(attr, err)
            except AquosCommandError as err:
                # The TV answered ERR. Once could be state-dependent; a run
                # of them is the model saying it doesn't do this. Then stop
                # spending a round trip on it.
                count = self._rejections.get(attr, 0) + 1
                self._rejections[attr] = count
                if count >= UNSUPPORTED_AFTER_REJECTIONS:
                    self._unsupported_fields.add(attr)
                    _LOGGER.warning(
                        "TV rejected %s %d times; treating it as unsupported "
                        "on this model and not polling it again: %s",
                        attr,
                        count,
                        err,
                    )
                else:
                    _LOGGER.debug("TV rejected %s (%d/%d): %s", attr, count, UNSUPPORTED_AFTER_REJECTIONS, err)
            else:
                self._supported_fields.add(attr)
                self._rejections.pop(attr, None)

        return data
