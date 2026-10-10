"""Minimal async client for Sharp's AQUOS IP/serial control protocol.

The protocol is a simple four-letter-command scheme over a plaintext TCP
connection (default port 10002): connect, authenticate with a username and
password (each terminated by \\r, blank if unset on the TV), then send a
four-character command code followed by a value left-padded to four
characters and terminated by \\r. The TV replies with "OK", "ERR", or the
requested value, also \\r-terminated. The TV does not keep the connection
open between commands well, so - matching Sharp's own documentation - a new
connection is opened for every command.
"""

from __future__ import annotations

import asyncio
import logging

_LOGGER = logging.getLogger(__name__)

# Commands that only make sense while the TV is on. Querying these while off
# either times out or returns garbage, so callers should skip them then.
STATUS_ONLY_WHEN_ON = frozenset(
    {"VOLM", "MUTE", "IAVD", "WIDE", "ACHN", "AVMD", "OPGD", "CHTR", "IFGS"}
)


class AquosError(Exception):
    """Base error talking to the TV."""


class AquosConnectionError(AquosError):
    """Could not open/authenticate a connection to the TV."""


class AquosCommandError(AquosError):
    """The TV answered the command with ERR: it rejected it."""


class AquosReplyError(AquosCommandError):
    """The TV answered, but with something unparseable (empty, truncated,
    a stray prompt). Subclasses AquosCommandError so existing handlers keep
    working, but callers that care can tell "rejected" from "garbled": the
    first is a fact about the model, the second is a fact about one
    exchange and should be retried, not remembered."""


# How long to wait for the TV to say anything after the login lines, and
# how long it must stay quiet before we consider that exchange finished.
# The TV may send nothing (no login configured), one prompt, two prompts,
# or an ERR per blank line - and in any number of TCP chunks. Draining
# until quiet handles all of those the same way; fixed-count reads do not.
_LOGIN_FIRST_WAIT = 0.6
_LOGIN_QUIET = 0.25


class AquosTV:
    """Thin async wrapper around the AQUOS four-letter-command protocol."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str = "",
        password: str = "",
        timeout: float = 3.0,
        retries: int = 2,
    ) -> None:
        self._host = host
        self._port = port
        # With no credentials there is nothing to log in with, and a real
        # TV (probed 2026-10-10) sends no prompt and answers each blank
        # line with its own ERR - two wasted replies per command that the
        # old fixed-count reads could mistake for the command's reply.
        # So send the login lines only when there is something in them.
        self._auth = f"{username}\r{password}\r".encode() if (username or password) else b""
        self._timeout = timeout
        self._retries = retries
        # One exchange at a time. The coordinator's poll and a service call
        # (or two service calls) otherwise open overlapping connections to
        # a TV that barely copes with one, and replies get attributed to
        # the wrong request. Created lazily so the client can be built
        # outside a running loop (config flow validation).
        self._lock: asyncio.Lock | None = None

    @staticmethod
    async def _drain_until_quiet(reader: asyncio.StreamReader, first_wait: float, quiet: float) -> bytes:
        """Read whatever arrives: wait up to first_wait for the first byte,
        then keep reading until the line goes quiet for `quiet` seconds or
        the TV closes the connection."""
        buf = b""
        wait = first_wait
        while True:
            try:
                chunk = await asyncio.wait_for(reader.read(1024), timeout=wait)
            except asyncio.TimeoutError:
                return buf
            if not chunk:
                return buf
            buf += chunk
            wait = quiet

    async def _read_reply(self, reader: asyncio.StreamReader) -> str:
        """Read the reply to a command: the first non-empty \\r-terminated
        line. Blank lines (a stray terminator from the login exchange) are
        skipped rather than returned as an empty reply."""
        deadline = asyncio.get_running_loop().time() + self._timeout
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            try:
                raw = await asyncio.wait_for(reader.readuntil(b"\r"), timeout=remaining)
            except asyncio.IncompleteReadError as err:
                # TV closed the connection mid-reply; whatever arrived is it.
                raw = err.partial
                line = raw.decode(errors="ignore").strip()
                if line:
                    return line
                raise AquosReplyError("TV closed the connection without replying") from err
            except asyncio.LimitOverrunError as err:
                raise AquosReplyError("TV reply had no terminator within buffer") from err
            line = raw.decode(errors="ignore").strip()
            if line:
                return line

    async def _send_once(self, code: str, value: str | int) -> str:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self._host, self._port), timeout=self._timeout
            )
        except (OSError, asyncio.TimeoutError) as err:
            raise AquosConnectionError(f"Could not connect to {self._host}:{self._port}") from err

        try:
            if self._auth:
                writer.write(self._auth)
                await writer.drain()
                # Swallow everything the TV says about the login lines -
                # prompts, echoes, an ERR per line, or nothing at all -
                # however it is chunked. The command's reply cannot arrive
                # before we send the command, so nothing drained here is
                # something we wanted.
                await self._drain_until_quiet(reader, _LOGIN_FIRST_WAIT, _LOGIN_QUIET)

            command = f"{code}{str(value).ljust(4)}\r"
            writer.write(command.encode())
            await writer.drain()
            reply = await self._read_reply(reader)
        except (OSError, asyncio.TimeoutError) as err:
            raise AquosConnectionError(f"Lost connection to {self._host}:{self._port}") from err
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

        return reply

    async def _send(self, code: str, value: str | int = "?") -> str | int | bool:
        """Send a command, retrying transient connection failures. Exchanges
        are serialised through the client lock."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            last_err: Exception | None = None
            for attempt in range(self._retries + 1):
                try:
                    status = await self._send_once(code, value)
                    break
                except AquosConnectionError as err:
                    last_err = err
                    if attempt < self._retries:
                        await asyncio.sleep(0.2)
            else:
                assert last_err is not None
                raise last_err

        if status == "OK":
            return True
        if status == "ERR":
            raise AquosCommandError(f"{code}{value} rejected by TV")
        try:
            return int(status)
        except ValueError:
            return status

    async def _send_int(self, code: str, value: str | int = "?") -> int:
        """Like _send, but raises AquosCommandError instead of letting a
        malformed/unsupported reply blow up as a bare ValueError.

        Every numeric field (volume, aspect ratio, backlight, etc.) goes
        through this. Without it, a TV replying with anything that isn't
        "OK"/"ERR"/a clean integer - e.g. a command it silently doesn't
        support - raised an uncaught ValueError that wasn't one of the
        AquosConnectionError/AquosCommandError types callers already handle,
        silently aborting whatever loop was awaiting it (and everything
        after it in that loop, with no log line at all).
        """
        result = await self._send(code, value)
        if isinstance(result, bool):
            # _send() returns True for a bare "OK" reply - never valid for
            # a numeric query.
            raise AquosReplyError(f"{code}{value} returned OK instead of a number")
        if isinstance(result, int):
            return result
        raise AquosReplyError(f"{code}{value} returned non-numeric reply: {result!r}")

    # --- Power / system -------------------------------------------------

    async def power(self, on: bool | None = None) -> bool:
        """Get or set power state.

        The query accepts only a clean 0 or 1. Anything else - "OK" (which
        _send returns as True, and True == 1), an empty line, a stray
        prompt - raises AquosReplyError so the caller keeps its last known
        state instead of flipping the TV on or off for one poll.
        """
        if on is None:
            state = await self._send_int("POWR")
            if state not in (0, 1):
                raise AquosReplyError(f"POWR? returned {state!r}, expected 0 or 1")
            return state == 1
        await self._send("POWR", 1 if on else 0)
        return on

    async def set_power_on_command_settings(self, tcp_wake_enabled: bool) -> None:
        """Enable/disable accepting a power-on command over TCP/IP (RSPW)."""
        await self._send("RSPW", 2 if tcp_wake_enabled else 0)

    async def model(self) -> str:
        return str(await self._send("MNRD"))

    # --- Audio / video ----------------------------------------------------

    async def volume(self, level: int | None = None) -> int:
        """Get or set volume, 0-60."""
        if level is None:
            return await self._send_int("VOLM")
        await self._send("VOLM", max(0, min(60, int(level))))
        return level

    async def mute(self, muted: bool | None = None) -> bool:
        """Get or set mute state. MUTE1 = muted, MUTE2 = unmuted (deterministic)."""
        if muted is None:
            return await self._send_int("MUTE") == 1
        await self._send("MUTE", 1 if muted else 2)
        return muted

    async def input_source(self, source_code: int | None = None) -> int:
        """Get or set the input (0=TV/Antenna via ITVD, 1-5 via IAVD)."""
        if source_code is None:
            return await self._send_int("IAVD")
        if source_code == 0:
            await self._send("ITVD", 0)
        else:
            await self._send("IAVD", source_code)
        return source_code

    async def aspect_ratio(self) -> int:
        return await self._send_int("WIDE")

    async def audio_selection(self) -> int:
        return await self._send_int("ACHN")

    async def av_mode(self) -> int:
        return await self._send_int("AVMD")

    async def backlight(self) -> int:
        return await self._send_int("OPGD")

    # --- Tuner ------------------------------------------------------------

    async def channel(self, number: int | None = None) -> int:
        """Get the current channel, or direct-tune to one (CHTR)."""
        if number is None:
            return await self._send_int("CHTR")
        await self._send("CHTR", number)
        return number

    async def channel_up(self) -> None:
        await self._send("CHUP", 1)

    async def channel_down(self) -> None:
        await self._send("CHDN", 1)

    async def signal_strength(self) -> int:
        return await self._send_int("IFGS")
