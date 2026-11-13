"""Home Assistant Bluetooth adapter for Petkit BLE integration."""
from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from typing import Any

from bleak_retry_connector import establish_connection
from homeassistant.components import bluetooth
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from .PetkitW5BLEMQTT.constants import Constants
from .PetkitW5BLEMQTT.utils import Utils

_LOGGER = logging.getLogger(__name__)

class ConnectionStatus(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    FAILED = "failed"

class HABluetoothAdapter:
    """Adapter to bridge HA's bluetooth integration with existing Petkit BLE library."""
    
    def __init__(
        self, hass: HomeAssistant, address: str, event_handler=None, logger=None,
        commands=None,
    ):
        """Initialize the HA Bluetooth adapter."""
        self.hass = hass
        self.address = address
        self.event_handler = event_handler
        self.commands = commands
        self.logger = logger or _LOGGER
        self.connected_devices = {}
        self.available_devices = {}
        self.connectiondata = {}
        self.queue = asyncio.Queue(10)
        self.callback = None
        self.device = False
        self._client = None
        self._ble_device = None
        
        # Connection status tracking (same as BLEManager)
        self._connection_status = ConnectionStatus.DISCONNECTED
        self._has_connected = False
        self._last_seen = None
        self._connection_attempts = 0
        self._last_connection_attempt = None
        self._connection_error = None
        self._retry_delay = 0.1  # Start with immediate retry (100ms)
        self._max_retry_delay = 30.0  # Maximum retry delay
        self._max_connection_attempts = 1000  # Very high to allow continuous retries
        self._last_logged_status = None  # Track last logged status to prevent spam
        self._last_reset_time = time.time()
        self._reset_interval = 300.0  # Reset connection attempts every 5 minutes
        self._immediate_reconnect = True  # Flag for immediate reconnection
        self._reconnection_task = None  # Track reconnection task
        self._connection_lock = asyncio.Lock()
        self._authentication_response = None

    async def scan(self) -> dict[str, Any]:
        """Scan for Petkit BLE devices using HA's bluetooth integration."""
        try:
            # Get discovered devices from HA's bluetooth integration
            discovered_devices = bluetooth.async_discovered_service_info(self.hass)
            
            # Filter for Petkit devices
            petkit_devices = {}
            for service_info in discovered_devices:
                is_configured_device = (
                    service_info.address.upper() == self.address.upper()
                )
                has_petkit_name = service_info.name and any(
                    device_type in service_info.name 
                    for device_type in ["W4", "W5", "CTW2"]
                )

                if is_configured_device or has_petkit_name:
                    device_address = (
                        self.address if is_configured_device else service_info.address
                    )
                    # Create a mock device object compatible with existing library
                    # Convert service_data to the format expected by the library
                    # The combine_byte_arrays function expects a dict with .values()
                    service_data_dict = {}
                    if service_info.service_data:
                        service_data_dict = service_info.service_data
                    else:
                        # Default service data with device type identifier for W5
                        service_data_dict = {"default": [0, 0, 0, 0, 0, 206]}  # 206 = W5 device type
                    
                    mock_device = type('MockDevice', (), {
                        'name': service_info.name or "Petkit Water Fountain",
                        'address': device_address,
                        'rssi': service_info.rssi,
                        'details': {
                            'props': {
                                'RSSI': service_info.rssi,
                                'ServiceData': service_data_dict
                            }
                        }
                    })()
                    
                    petkit_devices[device_address] = mock_device
                    self.connectiondata[device_address] = mock_device
                    
            self.available_devices = petkit_devices
            
            for address, device in petkit_devices.items():
                self.logger.info(f"Found HA BLE device: {device.name} ({address})")
                
            return petkit_devices
            
        except Exception as err:
            self.logger.error(f"Error scanning for devices: {err}")
            return {}

    async def connect_device(self, address: str) -> bool:
        """Connect to device using HA's bluetooth integration."""
        async with self._connection_lock:
            current_client = self.connected_devices.get(address)
            if current_client and getattr(current_client, "is_connected", False):
                return True

            if current_client:
                self.connected_devices.pop(address, None)
                self._fail_pending_writes()

                try:
                    await asyncio.wait_for(current_client.disconnect(), timeout=5.0)
                except Exception as err:
                    self.logger.debug(
                        "Failed to retire stale client for %s: %s", address, err
                    )

            return await self._connect_device(address)

    async def _connect_device(self, address: str) -> bool:
        """Establish a connection while holding the connection lock."""
        try:
            # Keep successful transparent reconnects out of the entity history.
            if not self._has_connected:
                self._update_connection_status(ConnectionStatus.CONNECTING)
                self.logger.info(f"🔄 Initial BLE connection attempt to {address}")
            elif self._connection_attempts:
                self._update_connection_status(ConnectionStatus.RECONNECTING)
                if self._connection_attempts % 10 == 0:  # Log every 10th attempt
                    self.logger.info(f"🔄 BLE reconnection attempt #{self._connection_attempts} to {address}")
            
            self._last_connection_attempt = time.time()
            
            # Get BLE device from HA's bluetooth integration
            self._ble_device = bluetooth.async_ble_device_from_address(
                self.hass, address, connectable=True
            )
            
            if not self._ble_device:
                error_msg = f"Device {address} not found in HA bluetooth scan"
                self._connection_attempts += 1
                
                if self._connection_attempts % 5 == 0:  # Log every 5th failure
                    self.logger.warning(f"⚠️ Device not found after {self._connection_attempts} attempts, will keep trying...")
                
                if not self._should_attempt_retry():
                    self._update_connection_status(ConnectionStatus.FAILED, error_msg)
                else:
                    self._update_connection_status(ConnectionStatus.RECONNECTING, error_msg)
                return False
            
            self.logger.debug("Device found in scan, establishing BLE connection...")
            
            # Use bleak-retry-connector directly with the BLE device
            from bleak import BleakClient
            client = await establish_connection(
                BleakClient,
                self._ble_device,
                address,
                disconnected_callback=self._handle_disconnected,
                timeout=10.0,
            )

            if not client.is_connected:
                raise ConnectionError("Client disconnected during connection setup")

            # A locally generated secret does not prove CMD 73 reached the device.
            # Leave unconfirmed pairing to the coordinator.
            if self.commands is not None and self.commands.registration_confirmed:
                try:
                    await self._authenticate_connection(client)

                    if not client.is_connected:
                        raise ConnectionError("Client disconnected during authentication")
                except (Exception, asyncio.CancelledError):
                    try:
                        await asyncio.wait_for(client.disconnect(), timeout=5.0)
                    except Exception as err:
                        self.logger.debug("Failed to close unauthenticated client: %s", err)
                    raise

            self._client = client
            self.connected_devices[address] = client
            self._has_connected = True
            self._update_connection_status(ConnectionStatus.CONNECTED)
            self._update_last_seen()
            
            self.logger.info(f"✅ BLE connection established to {address} after {self._connection_attempts + 1} attempt(s)")
            
            return True
            
        except asyncio.TimeoutError:
            self._connection_attempts += 1
            error_msg = f"Connection timeout (attempt #{self._connection_attempts})"
            
            if self._connection_attempts % 3 == 0:  # Log every 3rd timeout
                self.logger.warning(f"⏱️ Connection timeout after {self._connection_attempts} attempts, continuing...")
            
            if not self._should_attempt_retry():
                self._update_connection_status(ConnectionStatus.FAILED, error_msg)
            else:
                self._update_connection_status(ConnectionStatus.RECONNECTING, error_msg)
            
            return False
            
        except Exception as err:
            self._connection_attempts += 1
            error_msg = f"Connection attempt {self._connection_attempts} failed: {err}"
            
            if self._connection_attempts % 5 == 0:  # Log every 5th error
                self.logger.warning(f"❌ Connection failed {self._connection_attempts} times: {err}")
            
            if not self._should_attempt_retry():
                self._update_connection_status(ConnectionStatus.FAILED, error_msg)
            else:
                self._update_connection_status(ConnectionStatus.RECONNECTING, error_msg)
            
            return False

    async def _authenticate_connection(self, client) -> None:
        """Restore the Petkit session before exposing a reconnected client."""
        if self.commands is None or not hasattr(self.commands, "secret"):
            raise ConnectionError("Petkit session secret is not available")

        sequence = self.commands.sequence
        self.commands.increment_sequence()
        message = Utils.build_command(sequence, 86, 1, self.commands.secret)
        response = asyncio.get_running_loop().create_future()
        self._authentication_response = (sequence, response)

        try:
            async with asyncio.timeout(5.0):
                await client.start_notify(
                    Constants.READ_UUID, self._handle_notification_wrapper
                )
                # The consumer may itself be reconnecting, so do not use its queue.
                await client.write_gatt_char(Constants.WRITE_UUID, message, response=False)
                if not await response:
                    raise ConnectionError("Petkit session authentication was rejected")
            self.logger.debug("Petkit session authenticated after reconnect")
        finally:
            self._authentication_response = None
            if not response.done():
                response.cancel()

    async def disconnect_device(self, address: str, trigger_reconnect: bool = False) -> bool:
        """Disconnect from device.
        
        Args:
            address: Device address to disconnect
            trigger_reconnect: If True, immediately attempt reconnection
        """
        try:
            if client := self.connected_devices.pop(address, None):
                if hasattr(client, 'disconnect'):
                    await client.disconnect()
                self._update_connection_status(ConnectionStatus.DISCONNECTED)
                
                # Trigger immediate reconnection if requested and enabled
                if trigger_reconnect and self._immediate_reconnect:
                    self.logger.info("Triggering immediate reconnection after disconnect")
                    self.start_reconnection(address)
                
                return True
            return False
        except Exception as err:
            error_msg = f"Error disconnecting from {address}: {err}"
            # Force removal from connected devices even if disconnect fails
            if address in self.connected_devices:
                del self.connected_devices[address]
            self._update_connection_status(ConnectionStatus.DISCONNECTED, error_msg)
            
            if trigger_reconnect and self._immediate_reconnect:
                self.logger.info("Triggering reconnection after failed disconnect")
                self.start_reconnection(address)
            
            return False

    async def read_characteristic(self, address: str, characteristic_uuid: str) -> bytes | None:
        """Read characteristic using HA's bluetooth client."""
        try:
            if address in self.connected_devices:
                client = self.connected_devices[address]
                data = await client.read_gatt_char(characteristic_uuid)
                self.logger.info(f"Read data from {characteristic_uuid}: {data}")
                return data
            else:
                self.logger.error(f"Device {address} not connected")
                return None
        except Exception as err:
            self.logger.error(f"Error reading characteristic {characteristic_uuid}: {err}")
            return None

    async def write_characteristic(self, address: str, characteristic_uuid: str, data: bytes) -> bool:
        """Write characteristic using HA's bluetooth client."""
        client = None

        try:
            if address in self.connected_devices:
                client = self.connected_devices[address]
                # Check if client is still connected before attempting write
                if hasattr(client, 'is_connected') and not client.is_connected:
                    self.logger.warning(
                        "Client for %s reports not connected; reconnecting", address
                    )
                    await self._retire_client(
                        address, client, "Client disconnected during write"
                    )
                    return False
                    
                await client.write_gatt_char(
                    characteristic_uuid, data, response=False
                )
                self.logger.debug(f"Write complete to {characteristic_uuid}")
                self._update_last_seen()
                return True
            else:
                self.logger.debug(f"Device {address} not connected for write operation")
                # Attempt immediate reconnection if not connected
                if self._immediate_reconnect and self._connection_status != ConnectionStatus.CONNECTING:
                    self.start_reconnection(address)
                return False
        except Exception as err:
            error_msg = f"Write failed: {err}"
            self.logger.warning(
                "Error writing to characteristic %s: %s", characteristic_uuid, err
            )
            await self._retire_client(address, client, error_msg)
            return False

    def _handle_disconnected(self, client) -> None:
        """Schedule cleanup when Bleak reports a disconnected client."""
        asyncio.create_task(self._async_handle_disconnected(client))

    async def _async_handle_disconnected(self, client) -> None:
        """Clean up an unexpected disconnect and reconnect."""
        address = self.address
        if self.connected_devices.get(address) is not client:
            return

        self.connected_devices.pop(address, None)
        self._fail_pending_writes()
        self._connection_error = "Peripheral disconnected"

        if self._immediate_reconnect:
            self.start_reconnection(address)

    async def _retire_client(self, address: str, client, error: str) -> None:
        """Retire a failed client before starting a replacement connection."""
        if self.connected_devices.get(address) is client:
            self.connected_devices.pop(address, None)

        self._fail_pending_writes()
        self._connection_error = error

        if getattr(client, "is_connected", False):
            try:
                await asyncio.wait_for(client.disconnect(), timeout=5.0)
            except TimeoutError:
                self.logger.debug("Timed out disconnecting failed client for %s", address)
            except Exception as err:
                self.logger.debug(
                    "Failed to disconnect stale client for %s: %s", address, err
                )

        if self._immediate_reconnect:
            self.start_reconnection(address)

    def _fail_pending_writes(self) -> None:
        """Discard writes queued for a connection that has been lost."""
        while True:
            try:
                _, write_result = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                return

            if write_result is not None and not write_result.done():
                write_result.set_result(False)
            self.queue.task_done()

    async def start_notifications(self, address: str, characteristic_uuid: str) -> bool:
        """Start notifications using HA's bluetooth client."""
        try:
            if address in self.connected_devices:
                client = self.connected_devices[address]
                await client.start_notify(characteristic_uuid, self._handle_notification_wrapper)
                self.logger.info(f"Notifications started for {characteristic_uuid}")
                return True
            else:
                self.logger.error(f"Device {address} not connected")
                return False
        except Exception as err:
            self.logger.error(f"Error starting notifications for {characteristic_uuid}: {err}")
            return False

    async def stop_notifications(self, address: str, characteristic_uuid: str) -> bool:
        """Stop notifications using HA's bluetooth client."""
        try:
            if address in self.connected_devices:
                client = self.connected_devices[address]
                await client.stop_notify(characteristic_uuid)
                self.logger.info(f"Notifications stopped for {characteristic_uuid}")
                return True
            else:
                self.logger.error(f"Device {address} not connected")
                return False
        except Exception as err:
            self.logger.error(f"Error stopping notifications for {characteristic_uuid}: {err}")
            return False

    async def _handle_notification_wrapper(self, sender, data):
        """Wrapper for notification handling."""
        if self._authentication_response is not None and len(data) == 10:
            sequence, response = self._authentication_response
            if (
                data[:3] == b"\xfa\xfc\xfd"
                and data[3:8] == bytes([86, 2, sequence, 1, 0])
                and data[-1] == 251
                and not response.done()
            ):
                response.set_result(data[8] == 1)

        # Update last seen timestamp on successful notification
        self._update_last_seen()
        self.logger.debug(f"📨 Received BLE notification from {sender}: {data.hex() if data else 'None'}")
        if self.event_handler:
            try:
                await self.event_handler.handle_notification(sender, data)
                self.logger.debug("✅ Notification processed successfully")
            except Exception as err:
                self.logger.error(f"❌ Error processing notification: {err}")
        else:
            self.logger.warning("⚠️ No event handler configured for notifications")

    async def heartbeat(self, interval: int) -> None:
        """Heartbeat method compatible with existing library."""
        # This will be called by the existing Commands class
        # The actual heartbeat logic should be handled by HA's coordinator
        pass

    async def message_consumer(self, address: str, characteristic_uuid: str) -> None:
        """Message consumer compatible with existing library."""
        while True:
            try:
                if not self.connected_devices.get(address):
                    # Immediate reconnection if not connected
                    if (self._connection_status not in [ConnectionStatus.CONNECTING, ConnectionStatus.RECONNECTING] and
                        self._should_attempt_retry()):
                        
                        if self._immediate_reconnect:
                            # Use immediate reconnection loop instead of backoff
                            await self._immediate_reconnection_loop(address)
                        else:
                            await self._attempt_reconnection_with_backoff(address)
                    elif self._connection_status == ConnectionStatus.FAILED:
                        # Even if failed, keep trying with immediate reconnection
                        if self._immediate_reconnect:
                            self.logger.info("Connection previously failed, retrying immediately...")
                            # Reset attempts to allow more retries
                            self._connection_attempts = 0
                            await self._immediate_reconnection_loop(address)
                        else:
                            await asyncio.sleep(30)
                    
                    await asyncio.sleep(0.1)  # Very short sleep between checks
                    continue
                    
                message, write_result = await self.queue.get()

                try:
                    success = await self.write_characteristic(
                        address, characteristic_uuid, message
                    )
                    if success:
                        self._update_last_seen()

                    if write_result is not None and not write_result.done():
                        write_result.set_result(success)
                finally:
                    self.queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as err:
                self.logger.error(f"Error in message consumer: {err}")
                if self.queue.qsize() > 0:
                    self.queue.task_done()
                # Use immediate reconnection on error
                if self._immediate_reconnect:
                    await self._immediate_reconnection_loop(address)
                else:
                    await self._attempt_reconnection_with_backoff(address)
                
    async def _attempt_reconnection_with_backoff(self, address):
        """Attempt reconnection with exponential backoff."""
        if not self._should_attempt_retry():
            return
        
        retry_delay = self._calculate_retry_delay()
        
        # Only log the delay if status changed (prevents spam)
        if self._connection_status != ConnectionStatus.RECONNECTING:
            self.logger.info(f"Waiting {retry_delay:.1f}s before HA BLE reconnection attempt")
        
        await asyncio.sleep(retry_delay)
        
        success = await self.connect_device(address)
        if not success and self._should_attempt_retry():
            # Will be called again by the consumer loop
            pass

    async def message_producer(
        self, message: bytes, *, wait_for_write: bool = False
    ) -> bool:
        """Add message to queue for processing."""
        if not self.connected_devices.get(self.address):
            return False

        write_result = (
            asyncio.get_running_loop().create_future() if wait_for_write else None
        )
        await self.queue.put((message, write_result))

        if write_result is None:
            return True

        return await write_result
    
    @property
    def connection_status(self):
        """Get current connection status."""
        return self._connection_status.value
    
    @property
    def last_seen(self):
        """Get timestamp of last successful communication."""
        return self._last_seen
    
    @property
    def connection_attempts(self):
        """Get number of consecutive failed connection attempts."""
        return self._connection_attempts
    
    @property
    def connection_error(self):
        """Get last connection error message."""
        return self._connection_error
    
    def _update_connection_status(self, status, error=None):
        """Update connection status with controlled logging."""
        old_status = self._connection_status
        self._connection_status = status
        
        if error:
            self._connection_error = str(error)
        
        # Only log when status actually changes
        if old_status != status or self._last_logged_status != status:
            if status == ConnectionStatus.CONNECTED:
                self.logger.info(f"HA BLE Connection established - Status: {status.value}")
                self._connection_attempts = 0  # Reset on successful connection
                self._retry_delay = 1.0  # Reset retry delay
            elif status == ConnectionStatus.DISCONNECTED:
                self.logger.info(f"HA BLE Connection closed - Status: {status.value}")
            elif status == ConnectionStatus.CONNECTING:
                self.logger.info(f"HA BLE Attempting connection - Status: {status.value}")
            elif status == ConnectionStatus.RECONNECTING:
                self.logger.info(f"HA BLE Reconnecting (attempt {self._connection_attempts + 1}/{self._max_connection_attempts}) - Status: {status.value}")
            elif status == ConnectionStatus.FAILED:
                self.logger.error(f"HA BLE Connection failed after {self._max_connection_attempts} attempts - Status: {status.value}")
                if error:
                    self.logger.error(f"Last error: {error}")
            
            self._last_logged_status = status
    
    def _update_last_seen(self):
        """Update last seen timestamp."""
        self._last_seen = dt_util.now().isoformat()
    
    def _calculate_retry_delay(self):
        """Calculate exponential backoff delay."""
        if self._immediate_reconnect:
            if self._connection_attempts < 3:
                return 1.0
            elif self._connection_attempts < 10:
                return 2.0
            return 5.0
        else:
            # Original exponential backoff for non-immediate mode
            delay = min(self._retry_delay * (2 ** self._connection_attempts), self._max_retry_delay)
            return delay
    
    async def _immediate_reconnection_loop(self, address: str) -> None:
        """Immediately and continuously attempt to reconnect."""
        current_task = asyncio.current_task()

        if (
            self._reconnection_task
            and self._reconnection_task is not current_task
            and not self._reconnection_task.done()
        ):
            self.logger.debug("Reconnection already in progress, skipping duplicate loop")
            return

        self._reconnection_task = current_task

        try:
            while not self.connected_devices.get(address):
                try:
                    if not self._should_attempt_retry():
                        self.logger.info(
                            "Resetting connection attempts for continuous retry"
                        )
                        self._connection_attempts = 0
                        self._connection_status = ConnectionStatus.DISCONNECTED

                    if (
                        self._connection_attempts % 5 == 0
                        or self._connection_attempts < 3
                    ):
                        self.logger.info(
                            "Immediate reconnection attempt #%s",
                            self._connection_attempts + 1,
                        )

                    await asyncio.sleep(self._calculate_retry_delay())
                    success = await self.connect_device(address)

                    if success:
                        if self.commands is None or not self.commands.registration_confirmed:
                            await self.start_notifications(address, Constants.READ_UUID)

                        self.logger.info(
                            "Immediate reconnection successful after %s attempts",
                            self._connection_attempts,
                        )

                        break

                    if self._connection_attempts % 10 == 0:
                        self.logger.debug(
                            "Reconnection failed, retrying in %ss (attempt #%s)",
                            self._calculate_retry_delay(),
                            self._connection_attempts,
                        )

                except asyncio.CancelledError:
                    self.logger.info("Immediate reconnection loop cancelled")
                    raise
                except Exception as err:
                    self.logger.error("Error in immediate reconnection loop: %s", err)
                    await asyncio.sleep(0.5)
        finally:
            if self._reconnection_task is current_task:
                self._reconnection_task = None

    @property
    def reconnection_in_progress(self) -> bool:
        """Return whether a reconnection task is active."""
        return self._reconnection_task is not None and not self._reconnection_task.done()

    def start_reconnection(self, address: str) -> asyncio.Task:
        """Start one reconnection task or return the active task."""
        if self.reconnection_in_progress:
            return self._reconnection_task

        self._reconnection_task = asyncio.create_task(
            self._immediate_reconnection_loop(address)
        )
        return self._reconnection_task

    async def stop_reconnection(self) -> None:
        """Stop the active reconnection task."""
        task = self._reconnection_task
        if task is None or task is asyncio.current_task() or task.done():
            return

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass
    
    def _should_attempt_retry(self):
        """Check if we should attempt another retry."""
        # Auto-reset connection attempts if enough time has passed
        current_time = time.time()
        if (current_time - self._last_reset_time) >= self._reset_interval:
            if self._connection_attempts >= self._max_connection_attempts:
                self.logger.info(f"Auto-resetting connection attempts after {self._reset_interval}s timeout")
                self._connection_attempts = 0
                self._retry_delay = 1.0
                self._last_reset_time = current_time
                # Only reset to RECONNECTING if we were FAILED, otherwise keep current status
                if self._connection_status == ConnectionStatus.FAILED:
                    self._connection_status = ConnectionStatus.DISCONNECTED
        
        return self._connection_attempts < self._max_connection_attempts
    
    def reset_connection_state(self):
        """Reset connection tracking state for clean restart."""
        self._connection_status = ConnectionStatus.DISCONNECTED
        self._has_connected = False
        self._connection_attempts = 0
        self._retry_delay = 1.0
        self._connection_error = None
        self._last_connection_attempt = None
        self._last_logged_status = None
        self._last_reset_time = time.time()
