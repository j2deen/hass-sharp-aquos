"""Sensor platform for the Sharp Aquos TV integration.

Covers the extra readings the README promises beyond basic playback control:
aspect ratio, AV/picture mode, backlight level, and tuner signal strength.
Model is surfaced via the device info instead of a separate entity, and
audio-selection/channel aren't exposed here yet - their value mappings
haven't been confirmed against a real TV response.

Not every model answers every query, so a sensor is only created once the
coordinator has actually read that field off the TV. Fields the TV rejects
never get an entity, instead of a permanently-unknown sensor cluttering the
device page. Since support is only learned while the TV is on, entities may
appear a poll or two after setup rather than immediately.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from homeassistant.components.sensor import (
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import CONF_NAME, EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import AquosConfigEntry
from .const import ASPECT_RATIOS, AV_MODES
from .coordinator import AquosData
from .entity import AquosEntity


@dataclass(frozen=True, kw_only=True)
class AquosSensorEntityDescription(SensorEntityDescription):
    """Describes one AQUOS sensor and how to read it off the coordinator."""

    value_fn: Callable[[AquosData], str | int | None]


SENSOR_DESCRIPTIONS: tuple[AquosSensorEntityDescription, ...] = (
    AquosSensorEntityDescription(
        key="aspect_ratio",
        translation_key="aspect_ratio",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: (
            ASPECT_RATIOS.get(data.aspect_ratio, data.aspect_ratio)
            if data.aspect_ratio is not None
            else None
        ),
    ),
    AquosSensorEntityDescription(
        key="av_mode",
        translation_key="av_mode",
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=lambda data: (
            AV_MODES.get(data.av_mode, data.av_mode) if data.av_mode is not None else None
        ),
    ),
    AquosSensorEntityDescription(
        key="backlight",
        translation_key="backlight",
        entity_category=EntityCategory.DIAGNOSTIC,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data.backlight,
    ),
    AquosSensorEntityDescription(
        key="signal_strength",
        translation_key="signal_strength",
        entity_category=EntityCategory.DIAGNOSTIC,
        state_class=SensorStateClass.MEASUREMENT,
        value_fn=lambda data: data.signal_strength,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry: AquosConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up AQUOS diagnostic sensors from a config entry.

    Sensors are added lazily as the coordinator confirms the TV supports
    each field, so a model that rejects e.g. the backlight query simply
    never gets a backlight sensor.
    """
    coordinator = entry.runtime_data
    added: set[str] = set()

    @callback
    def _add_supported_sensors() -> None:
        new_entities = [
            AquosSensor(entry, description)
            for description in SENSOR_DESCRIPTIONS
            if description.key not in added
            and description.key in coordinator.supported_fields
        ]
        if not new_entities:
            return
        added.update(entity.entity_description.key for entity in new_entities)
        async_add_entities(new_entities)

    _add_supported_sensors()
    entry.async_on_unload(coordinator.async_add_listener(_add_supported_sensors))


class AquosSensor(AquosEntity, SensorEntity):
    """A single read-only diagnostic value read off the TV."""

    entity_description: AquosSensorEntityDescription

    def __init__(
        self, entry: AquosConfigEntry, description: AquosSensorEntityDescription
    ) -> None:
        super().__init__(entry.runtime_data, entry.entry_id, entry.data[CONF_NAME])
        self.entity_description = description
        self._attr_unique_id = f"{entry.entry_id}_{description.key}"

    @property
    def native_value(self) -> str | int | None:
        return self.entity_description.value_fn(self.coordinator.data)
