"""Petkit BLE Water Fountain integration for Home Assistant."""

from __future__ import annotations

import logging

import voluptuous as vol

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers.service import async_extract_config_entry_ids
from homeassistant.helpers.typing import ConfigType

from .const import DOMAIN
from .coordinator import PetkitBLECoordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.BINARY_SENSOR,
    Platform.BUTTON,
    Platform.SENSOR,
    Platform.SWITCH,
]

SERVICE_SET_DEVICE_CONFIG = "set_device_config"
TARGET_FIELDS = {"area_id", "device_id", "entity_id", "floor_id", "label_id"}

SERVICE_SET_DEVICE_CONFIG_SCHEMA = vol.Schema(
    {
        **cv.TARGET_SERVICE_FIELDS,
        vol.Optional("smart_time_on"): cv.positive_int,
        vol.Optional("smart_time_off"): cv.positive_int,
        vol.Optional("led_brightness"): vol.All(
            vol.Coerce(int), vol.Range(min=0, max=100)
        ),
    }
)


async def _coordinators_for_service_call(
    hass: HomeAssistant, call: ServiceCall
) -> list[PetkitBLECoordinator]:
    """Return coordinators targeted by a service call."""
    coordinators: dict[str, PetkitBLECoordinator] = hass.data.get(DOMAIN, {})
    target_entry_ids = await async_extract_config_entry_ids(call)

    if not target_entry_ids:
        if len(coordinators) == 1 and not TARGET_FIELDS.intersection(call.data):
            return list(coordinators.values())

        raise HomeAssistantError("No Petkit water fountain found for the target")

    targeted_coordinators = [
        coordinator
        for entry_id, coordinator in coordinators.items()
        if entry_id in target_entry_ids
    ]

    if not targeted_coordinators:
        raise HomeAssistantError("No Petkit water fountain found for the target")

    return targeted_coordinators


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up Petkit BLE services."""

    async def handle_set_device_config(call: ServiceCall) -> None:
        """Handle the set device config service call."""
        for coordinator in await _coordinators_for_service_call(hass, call):
            current_config = coordinator.device.config
            config_data = [
                call.data.get("smart_time_on", current_config.get("smart_time_on", 30)),
                call.data.get(
                    "smart_time_off", current_config.get("smart_time_off", 60)
                ),
                current_config.get("led_switch", 1),
                call.data.get(
                    "led_brightness", current_config.get("led_brightness", 80)
                ),
                current_config.get("led_on_byte1", 0),
                current_config.get("led_on_byte2", 0),
                current_config.get("led_off_byte1", 0),
                current_config.get("led_off_byte2", 0),
                current_config.get("do_not_disturb_switch", 0),
                current_config.get("dnd_on_byte1", 0),
                current_config.get("dnd_on_byte2", 0),
                current_config.get("dnd_off_byte1", 0),
                current_config.get("dnd_off_byte2", 0),
                current_config.get("is_locked", 0),
            ]

            await coordinator.async_set_device_config(config_data)
            await coordinator.async_request_refresh()

    hass.services.async_register(
        DOMAIN,
        SERVICE_SET_DEVICE_CONFIG,
        handle_set_device_config,
        schema=SERVICE_SET_DEVICE_CONFIG_SCHEMA,
    )

    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Petkit BLE from a config entry."""
    coordinator = PetkitBLECoordinator(hass, entry)

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Start the coordinator (this replaces async_config_entry_first_refresh for ActiveBluetoothProcessorCoordinator)
    entry.async_create_task(hass, coordinator.async_start())

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        coordinator = hass.data[DOMAIN].pop(entry.entry_id)
        await coordinator.async_shutdown()

    return unload_ok
