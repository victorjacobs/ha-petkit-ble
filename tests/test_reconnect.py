"""Exercise the real adapter with only the HA/Bleak boundaries replaced."""

import asyncio
import importlib
import logging
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch


INTEGRATION = Path(__file__).resolve().parents[1] / "custom_components" / "petkit_ble"


def load_adapter():
    package = ModuleType("petkit_reconnect_tests")
    package.__path__ = [str(INTEGRATION)]
    library = ModuleType("petkit_reconnect_tests.PetkitW5BLEMQTT")
    library.__path__ = [str(INTEGRATION / "PetkitW5BLEMQTT")]
    bluetooth = ModuleType("homeassistant.components.bluetooth")
    bluetooth.async_ble_device_from_address = Mock(return_value=object())
    components = ModuleType("homeassistant.components")
    components.bluetooth = bluetooth
    core = ModuleType("homeassistant.core")
    core.HomeAssistant = object
    util = ModuleType("homeassistant.util")
    util.dt = SimpleNamespace(now=Mock(return_value=SimpleNamespace(isoformat=lambda: "now")))
    connector = ModuleType("bleak_retry_connector")
    connector.establish_connection = AsyncMock()
    bleak = ModuleType("bleak")
    bleak.BleakClient = object

    modules = {
        package.__name__: package,
        library.__name__: library,
        "homeassistant": ModuleType("homeassistant"),
        components.__name__: components,
        bluetooth.__name__: bluetooth,
        core.__name__: core,
        util.__name__: util,
        connector.__name__: connector,
        bleak.__name__: bleak,
    }
    with patch.dict(sys.modules, modules):
        adapter = importlib.import_module(f"{package.__name__}.ha_bluetooth_adapter")
        commands = importlib.import_module(f"{library.__name__}.commands")
        device = importlib.import_module(f"{library.__name__}.device")
        utils = importlib.import_module(f"{library.__name__}.utils")
        constants = importlib.import_module(f"{library.__name__}.constants")
        handlers = importlib.import_module(f"{library.__name__}.event_handlers")

    return (
        adapter, commands.Commands, device.Device, utils.Utils, constants.Constants,
        bleak, handlers.EventHandlers,
    )


adapter_module, Commands, Device, Utils, Constants, bleak_module, EventHandlers = load_adapter()


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.address = "AA:BB:CC:DD:EE:FF"
        self.device = Device(self.address)
        self.commands = Commands(None, self.device, logging.getLogger(__name__))
        self.device.device_id_bytes = [0, 0, 0, 0, 0, 0]
        self.event_handler = EventHandlers(
            self.device, self.commands, logging.getLogger(__name__)
        )
        self.commands.ble_manager = SimpleNamespace(message_producer=AsyncMock())
        await self.commands.init_device()
        await self.event_handler.handle_notification(None, Utils.build_command(0, 73, 2, [1]))
        self.commands.sequence = 0
        self.registered_secret = self.commands.secret.copy()
        self.adapter = adapter_module.HABluetoothAdapter(
            object(), self.address, logger=logging.getLogger(__name__),
            event_handler=self.event_handler,
            commands=self.commands,
        )
        self.commands.ble_manager = self.adapter
        self.adapter._has_connected = True
        self.authenticated = False
        self.notification_callback = None
        self.writes = []
        self.auth_reply = asyncio.Event()
        self.release_auth = asyncio.Event()
        self.release_auth.set()
        self.auth_result = 1

        async def start_notify(uuid, callback):
            self.assertEqual(uuid, Constants.READ_UUID)
            self.notification_callback = callback

        async def write(uuid, message, *, response):
            self.assertFalse(response)
            packet = Utils.parse_bytearray(message)
            self.writes.append(packet)
            if packet["cmd"] == 73:
                self.assertEqual(len(packet["data"]), 16)
                self.registered_secret = list(packet["data"][8:])
                self.authenticated = True
                await self.notification_callback(
                    None, Utils.build_command(packet["seq"], 73, 2, [1])
                )
            elif packet["cmd"] == 86:
                self.assertIsNotNone(self.notification_callback)
                self.assertEqual(list(packet["data"]), self.commands.secret)
                self.auth_reply.set()
                await self.release_auth.wait()
                result = int(
                    self.auth_result == 1
                    and list(packet["data"]) == self.registered_secret
                )
                self.authenticated = result == 1
                await self.notification_callback(
                    None, Utils.build_command(packet["seq"], 86, 2, [result])
                )
            elif packet["cmd"] == 222 and self.authenticated:
                self.device.status = {"filter_percentage": 1.0, "warning_filter": 0}

        self.client = SimpleNamespace(
            is_connected=True,
            start_notify=AsyncMock(side_effect=start_notify),
            write_gatt_char=AsyncMock(side_effect=write),
            disconnect=AsyncMock(),
        )
        self.connection_patch = patch.object(
            adapter_module, "establish_connection", AsyncMock(return_value=self.client)
        )
        self.connection_patch.start()
        self.addCleanup(self.connection_patch.stop)
        self.bleak_patch = patch.dict(sys.modules, {"bleak": bleak_module})
        self.bleak_patch.start()
        self.addCleanup(self.bleak_patch.stop)

    async def test_dropped_registration_while_disconnected_can_pair_then_authenticate(self):
        self.commands = Commands(self.adapter, self.device, logging.getLogger(__name__))
        self.adapter.commands = self.commands
        self.event_handler = EventHandlers(
            self.device, self.commands, logging.getLogger(__name__)
        )
        self.adapter.event_handler = self.event_handler
        self.adapter._has_connected = False
        self.adapter._immediate_reconnect = False
        self.registered_secret = None
        self.authenticated = False
        self.assertTrue(await self.adapter.connect_device(self.address))

        self.client.is_connected = False
        await self.adapter._async_handle_disconnected(self.client)
        await self.commands.init_device()
        self.assertTrue(hasattr(self.commands, "secret"))
        self.assertTrue(self.adapter.queue.empty())
        self.assertEqual(self.writes, [])

        self.client.is_connected = True
        self.assertTrue(await self.adapter.connect_device(self.address))
        self.assertEqual(self.writes, [])
        self.assertFalse(self.commands.registration_confirmed)
        self.assertTrue(await self.adapter.start_notifications(self.address, Constants.READ_UUID))
        consumer = asyncio.create_task(
            self.adapter.message_consumer(self.address, Constants.WRITE_UUID)
        )
        try:
            await self.commands.init_device()
            await asyncio.wait_for(self.adapter.queue.join(), timeout=1)
        finally:
            consumer.cancel()
            await consumer

        self.assertTrue(self.commands.registration_confirmed)
        self.assertEqual(self.device.device_initialized, 1)
        self.assertEqual(self.registered_secret, self.commands.secret)
        self.client.is_connected = False
        await self.adapter._async_handle_disconnected(self.client)
        self.client.is_connected = True
        self.assertTrue(await self.adapter.connect_device(self.address))
        self.assertEqual([packet["cmd"] for packet in self.writes], [73, 86])

    async def test_filter_reset_works_after_reconnect_without_ha_restart(self):
        self.device.status = {"filter_percentage": 0.0, "warning_filter": 1}
        self.assertTrue(await self.adapter.connect_device(self.address))
        consumer = asyncio.create_task(
            self.adapter.message_consumer(self.address, Constants.WRITE_UUID)
        )
        try:
            await asyncio.wait_for(self.commands.set_reset_filter(), timeout=1)
        finally:
            consumer.cancel()
            await consumer

        self.assertEqual(self.device.status["filter_percentage"], 1.0)
        self.assertEqual(self.device.status["warning_filter"], 0)
        self.assertEqual([packet["cmd"] for packet in self.writes], [86, 222])

    async def test_registration_and_sync_use_the_same_eight_byte_secret(self):
        producer = AsyncMock()
        self.commands.ble_manager = SimpleNamespace(message_producer=producer)
        self.device.device_id_bytes = [0, 0, 0, 0, 0, 0]

        await self.commands.init_device()
        registration = Utils.parse_bytearray(producer.call_args.args[0])
        await self.commands.get_device_sync()
        authentication = Utils.parse_bytearray(producer.call_args.args[0])

        self.assertEqual(len(registration["data"]), 16)
        self.assertEqual(list(registration["data"][8:]), self.commands.secret)
        self.assertEqual(list(authentication["data"]), self.commands.secret)

    async def test_client_is_not_exposed_before_authentication_reply(self):
        self.release_auth.clear()
        connecting = asyncio.create_task(self.adapter.connect_device(self.address))
        try:
            await asyncio.wait_for(self.auth_reply.wait(), timeout=1)
            self.assertNotIn(self.address, self.adapter.connected_devices)
            self.assertFalse(await self.adapter.message_producer(b"not authenticated"))
        finally:
            self.release_auth.set()
            await connecting

        self.assertIs(self.adapter.connected_devices[self.address], self.client)

    async def test_rejected_authentication_closes_client_and_allows_retry(self):
        self.auth_result = 0
        self.assertFalse(await self.adapter.connect_device(self.address))
        self.assertNotIn(self.address, self.adapter.connected_devices)
        self.client.disconnect.assert_awaited_once()
        self.assertIsNone(self.adapter._authentication_response)

        self.auth_result = 1
        self.assertTrue(await self.adapter.connect_device(self.address))
        self.assertEqual([packet["seq"] for packet in self.writes], [0, 1])

    async def test_notification_subscription_is_bounded_before_authentication(self):
        subscribed = asyncio.Event()

        async def stalled_notify(*args):
            subscribed.set()
            await asyncio.Event().wait()

        self.client.start_notify.side_effect = stalled_notify
        real_timeout = asyncio.timeout
        with patch.object(adapter_module.asyncio, "timeout", lambda _: real_timeout(0.01)):
            connecting = asyncio.create_task(self.adapter.connect_device(self.address))
            await subscribed.wait()
            try:
                done, _ = await asyncio.wait({connecting}, timeout=0.1)
                self.assertIn(connecting, done, "notification subscription bypassed auth timeout")
                self.assertFalse(await connecting)
            finally:
                if not connecting.done():
                    connecting.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await connecting

        self.client.disconnect.assert_awaited_once()
        self.assertNotIn(self.address, self.adapter.connected_devices)
        self.client.write_gatt_char.assert_not_awaited()
        self.assertIsNone(self.adapter._authentication_response)

    async def test_missing_authentication_reply_closes_client(self):
        self.client.write_gatt_char.side_effect = None
        real_timeout = asyncio.timeout

        with patch.object(
            adapter_module.asyncio, "timeout", lambda _: real_timeout(0.01)
        ):
            self.assertFalse(await self.adapter.connect_device(self.address))

        self.assertNotIn(self.address, self.adapter.connected_devices)
        self.client.disconnect.assert_awaited_once()
        self.assertIsNone(self.adapter._authentication_response)

    async def test_unrelated_notifications_do_not_authenticate_client(self):
        self.release_auth.clear()
        connecting = asyncio.create_task(self.adapter.connect_device(self.address))
        try:
            await asyncio.wait_for(self.auth_reply.wait(), timeout=1)
            sequence, response = self.adapter._authentication_response
            for message in (
                Utils.build_command(sequence + 1, 86, 2, [1]),
                Utils.build_command(sequence, 73, 2, [1]),
                Utils.build_command(sequence, 86, 1, [1]),
                Utils.build_command(sequence, 86, 2, []),
                Utils.build_command(sequence, 86, 2, [1])[:-1],
            ):
                await self.adapter._handle_notification_wrapper(None, message)
                self.assertFalse(response.done())
        finally:
            self.release_auth.set()
            await connecting

    async def test_cancelled_authentication_closes_client(self):
        self.release_auth.clear()
        connecting = asyncio.create_task(self.adapter.connect_device(self.address))
        await asyncio.wait_for(self.auth_reply.wait(), timeout=1)
        connecting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await connecting

        self.client.disconnect.assert_awaited_once()
        self.assertNotIn(self.address, self.adapter.connected_devices)
        self.assertIsNone(self.adapter._authentication_response)

    async def test_concurrent_reconnects_share_one_authenticated_client(self):
        self.release_auth.clear()
        first = asyncio.create_task(self.adapter.connect_device(self.address))
        second = asyncio.create_task(self.adapter.connect_device(self.address))
        try:
            await asyncio.wait_for(self.auth_reply.wait(), timeout=1)
            self.assertFalse(first.done())
            self.assertFalse(second.done())
            self.assertNotIn(self.address, self.adapter.connected_devices)
        finally:
            self.release_auth.set()
            results = await asyncio.wait_for(asyncio.gather(first, second), timeout=1)

        self.assertEqual(results, [True, True])
        adapter_module.establish_connection.assert_awaited_once()
        self.client.start_notify.assert_awaited_once()
        self.assertEqual([packet["cmd"] for packet in self.writes], [86])
        self.assertIs(self.adapter.connected_devices[self.address], self.client)

    async def test_disconnect_during_authentication_does_not_publish_client(self):
        self.release_auth.clear()
        connecting = asyncio.create_task(self.adapter.connect_device(self.address))
        try:
            await asyncio.wait_for(self.auth_reply.wait(), timeout=1)
            self.client.is_connected = False
            await self.adapter._async_handle_disconnected(self.client)
        finally:
            self.release_auth.set()
            connected = await asyncio.wait_for(connecting, timeout=1)

        self.assertFalse(connected)
        self.assertNotIn(self.address, self.adapter.connected_devices)
        self.client.disconnect.assert_awaited_once()
        self.assertIsNone(self.adapter._authentication_response)

    async def test_initial_connection_still_uses_coordinator_initialization(self):
        self.commands.registration_confirmed = False
        self.adapter._has_connected = False
        self.assertTrue(await self.adapter.connect_device(self.address))
        self.client.start_notify.assert_not_awaited()
        self.client.write_gatt_char.assert_not_awaited()

    async def test_reconnect_before_pairing_restores_notifications(self):
        del self.commands.secret
        self.commands.registration_confirmed = False
        self.adapter._calculate_retry_delay = lambda: 0
        await asyncio.wait_for(self.adapter._immediate_reconnection_loop(self.address), timeout=1)

        self.client.start_notify.assert_awaited_once()
        self.client.write_gatt_char.assert_not_awaited()

    async def test_interrupted_initial_connection_can_retry_initialization(self):
        del self.commands.secret
        self.commands.registration_confirmed = False
        self.adapter._has_connected = False
        self.adapter._immediate_reconnect = False
        self.assertTrue(await self.adapter.connect_device(self.address))
        self.assertFalse(hasattr(self.commands, "secret"))

        self.client.is_connected = False
        await self.adapter._async_handle_disconnected(self.client)
        self.assertNotIn(self.address, self.adapter.connected_devices)
        self.client.is_connected = True

        self.assertTrue(await self.adapter.connect_device(self.address))
        self.client.start_notify.assert_not_awaited()
        self.client.write_gatt_char.assert_not_awaited()
        self.assertTrue(
            await self.adapter.start_notifications(self.address, Constants.READ_UUID)
        )
        self.device.device_id_bytes = [0, 0, 0, 0, 0, 0]
        consumer = asyncio.create_task(
            self.adapter.message_consumer(self.address, Constants.WRITE_UUID)
        )
        try:
            await self.commands.init_device()
            await asyncio.wait_for(self.adapter.queue.join(), timeout=1)
        finally:
            consumer.cancel()
            await consumer

        self.client.start_notify.assert_awaited_once()
        self.assertEqual([packet["cmd"] for packet in self.writes], [73])
        secret = self.commands.secret.copy()

        self.client.is_connected = False
        await self.adapter._async_handle_disconnected(self.client)
        self.client.is_connected = True
        self.assertTrue(await self.adapter.connect_device(self.address))
        self.assertEqual([packet["cmd"] for packet in self.writes], [73, 86])
        self.assertEqual(self.commands.secret, secret)

    async def test_reconnect_authentication_reserves_sequence_before_writing(self):
        self.commands.sequence = 255
        self.assertTrue(await self.adapter.connect_device(self.address))
        self.assertEqual(self.writes[0]["seq"], 255)
        self.assertEqual(self.commands.sequence, 0)

    async def test_consumer_can_reconnect_without_waiting_on_its_own_queue(self):
        self.adapter._calculate_retry_delay = lambda: 0
        consumer = asyncio.create_task(
            self.adapter.message_consumer(self.address, Constants.WRITE_UUID)
        )
        try:
            await asyncio.wait_for(self.auth_reply.wait(), timeout=1)
            await asyncio.wait_for(self.commands.set_reset_filter(), timeout=1)
            self.assertEqual(self.device.status["filter_percentage"], 1.0)
            self.client.start_notify.assert_awaited_once()
        finally:
            consumer.cancel()
            await consumer


if __name__ == "__main__":
    unittest.main()
