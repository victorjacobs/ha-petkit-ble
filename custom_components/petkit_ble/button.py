"""Button platform for Petkit BLE integration."""

from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import PetkitBLECoordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Petkit BLE button."""
    coordinator: PetkitBLECoordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities([PetkitResetFilterButton(coordinator)])


class PetkitResetFilterButton(CoordinatorEntity[PetkitBLECoordinator], ButtonEntity):
    """Reset the filter life for one water fountain."""

    _attr_has_entity_name = True
    _attr_icon = "mdi:air-filter"
    _attr_translation_key = "reset_filter"

    def __init__(self, coordinator: PetkitBLECoordinator) -> None:
        """Initialize the button."""
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.address.replace(':', '')}_reset_filter"

    @property
    def device_info(self) -> DeviceInfo:
        """Return device info."""
        return self.coordinator.device_info

    async def async_press(self) -> None:
        """Reset the filter life."""
        await self.coordinator.async_reset_filter()
        await self.coordinator.async_request_refresh()
