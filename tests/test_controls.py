"""Control recovery at the real coordinator's HA boundary."""

import asyncio
import importlib
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from test_reconnect import INTEGRATION, adapter_module


class UpdateFailed(Exception):
    """Stand-in for HA's control error."""


class BluetoothCoordinator:
    def __class_getitem__(cls, item):
        return cls


def load_coordinator():
    package = ModuleType("petkit_control_tests")
    package.__path__ = [str(INTEGRATION)]
    library = ModuleType("PetkitW5BLEMQTT")
    library.__path__ = [str(INTEGRATION / "PetkitW5BLEMQTT")]
    modules = {
        package.__name__: package,
        library.__name__: library,
        f"{package.__name__}.ha_bluetooth_adapter": adapter_module,
        "homeassistant": ModuleType("homeassistant"),
        "homeassistant.components": SimpleNamespace(bluetooth=SimpleNamespace()),
        "homeassistant.components.bluetooth": ModuleType("bluetooth"),
        "homeassistant.components.bluetooth.active_update_processor": SimpleNamespace(
            ActiveBluetoothProcessorCoordinator=BluetoothCoordinator
        ),
        "homeassistant.config_entries": SimpleNamespace(ConfigEntry=object),
        "homeassistant.core": SimpleNamespace(HomeAssistant=object),
        "homeassistant.helpers": SimpleNamespace(device_registry=Mock()),
        "homeassistant.helpers.entity": SimpleNamespace(DeviceInfo=dict),
        "homeassistant.helpers.update_coordinator": SimpleNamespace(UpdateFailed=UpdateFailed),
    }
    with patch.dict(sys.modules, modules):
        return importlib.import_module(f"{package.__name__}.coordinator").PetkitBLECoordinator


PetkitBLECoordinator = load_coordinator()


class ControlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.coordinator = object.__new__(PetkitBLECoordinator)
        self.coordinator.address = "AA:BB:CC:DD:EE:FF"
        self.coordinator._initialized = True
        self.coordinator.commands = SimpleNamespace(set_reset_filter=AsyncMock())
        self.coordinator.async_update_listeners = Mock()
        self.coordinator.ble_manager = SimpleNamespace(
            connected_devices={},
            connect_device=AsyncMock(return_value=True),
            connection_error="Petkit session registration was rejected",
            reconnection_in_progress=True,
            start_reconnection=Mock(),
        )

    async def test_reset_waits_for_connection_before_sending_command(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def connect(address):
            started.set()
            await release.wait()
            return True

        self.coordinator.ble_manager.connect_device.side_effect = connect
        resetting = asyncio.create_task(self.coordinator.async_reset_filter())
        try:
            await asyncio.wait_for(started.wait(), timeout=0.1)
            self.coordinator.commands.set_reset_filter.assert_not_awaited()
        finally:
            release.set()
            await resetting

        self.coordinator.commands.set_reset_filter.assert_awaited_once()

    async def test_reset_reports_failed_connection_without_sending_command(self):
        self.coordinator.ble_manager.connect_device.return_value = False
        with self.assertRaisesRegex(UpdateFailed, "registration was rejected"):
            await self.coordinator.async_reset_filter()
        self.coordinator.commands.set_reset_filter.assert_not_awaited()

    async def test_reset_connection_wait_is_bounded(self):
        async def never_connect(address):
            await asyncio.Event().wait()

        self.coordinator.ble_manager.connect_device.side_effect = never_connect
        real_timeout = asyncio.timeout
        with patch.object(asyncio, "timeout", lambda _: real_timeout(0.01)):
            with self.assertRaisesRegex(UpdateFailed, "Timed out"):
                await self.coordinator.async_reset_filter()
        self.coordinator.commands.set_reset_filter.assert_not_awaited()

    async def test_disconnected_refresh_publishes_current_connection_status(self):
        await self.coordinator._async_request_refresh()
        self.coordinator.async_update_listeners.assert_called_once()
        self.coordinator.ble_manager.start_reconnection.assert_not_called()
