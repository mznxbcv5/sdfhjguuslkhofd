# api.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Optional, Set
import json
import asyncio
import datetime
import logging
import time
import aiohttp
import os
from contextlib import asynccontextmanager
from collections import deque

# ============================================================
# تنظیمات
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PHP_API_URL = "http://egzozereza.ir/data.php"

PING_INTERVAL = 20
PING_TIMEOUT = 60
MAX_NOTIFICATIONS_CACHE = 200

# ============================================================
# کلاس مدیریت اتصالات و کش
# ============================================================
class ConnectionManager:
    def __init__(self):
        self.devices: Dict[str, WebSocket] = {}          # set_code -> WebSocket
        self.device_connect_time: Dict[str, float] = {}  # زمان آخرین دریافت پیام (حتی پینگ)
        self.bot_connections: Dict[str, WebSocket] = {}
        self.is_bot_connected = False
        self.api_session: Optional[aiohttp.ClientSession] = None
        self.pending_notifications: deque = deque(maxlen=MAX_NOTIFICATIONS_CACHE)
        self.connecting_devices: Set[str] = set()
        self._lock = asyncio.Lock()

    async def init_session(self):
        if self.api_session is None:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.api_session = aiohttp.ClientSession(timeout=timeout)

    async def close_session(self):
        if self.api_session:
            await self.api_session.close()
            self.api_session = None

    # ★ Pry-فقط برای خواندن اطلاعات دستگاه‌ها از دیتابیس (برای نمایش و آمار)
    async def api_request(self, endpoint: str, method: str = 'GET', data: dict = None, retries: int = 2) -> Optional[dict]:
        url = f"{PHP_API_URL}/{endpoint.lstrip('/')}"
        headers = {"Content-Type": "application/json"}

        for attempt in range(retries + 1):
            try:
                if method.upper() == 'GET':
                    async with self.api_session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        logger.error(f"API GET error: {resp.status}")
                elif method.upper() == 'POST':
                    async with self.api_session.post(url, json=data, headers=headers) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        logger.error(f"API POST error: {resp.status}")
                elif method.upper() == 'DELETE':
                    async with self.api_session.delete(url, headers=headers) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        logger.error(f"API DELETE error: {resp.status}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"API request attempt {attempt+1} failed: {e}")
                if attempt < retries:
                    await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"API request error: {e}")
                break
        return None

    def is_device_online(self, set_code: str) -> bool:
        return set_code in self.devices

    async def send_to_device(self, set_code: str, message: dict) -> bool:
        if set_code in self.devices:
            try:
                await self.devices[set_code].send_json(message)
                return True
            except Exception as e:
                logger.error(f"Send to device {set_code} failed: {e}")
                await self.disconnect_device(set_code)
                return False
        return False

    async def send_to_bot(self, message: dict) -> bool:
        if self.bot_connections:
            try:
                for ws in list(self.bot_connections.values()):
                    try:
                        await ws.send_json(message)
                    except Exception:
                        pass
                return True
            except Exception:
                return False
        return False

    async def connect_device(self, set_code: str, websocket: WebSocket):
        async with self._lock:
            if set_code in self.connecting_devices:
                logger.warning(f"⚠️ Device {set_code} already connecting, closing duplicate")
                await websocket.close(code=1000, reason="Duplicate connection")
                return False

            if set_code in self.devices:
                logger.warning(f"⚠️ Device {set_code} already connected, closing old connection")
                try:
                    await self.devices[set_code].close(code=1000, reason="New connection")
                except:
                    pass
                try:
                    del self.devices[set_code]
                except KeyError:
                    pass
                if set_code in self.device_connect_time:
                    try:
                        del self.device_connect_time[set_code]
                    except KeyError:
                        pass

            self.connecting_devices.add(set_code)

        try:
            await websocket.accept()
            async with self._lock:
                self.devices[set_code] = websocket
                self.device_connect_time[set_code] = time.time()

            # ارسال نوتیفیکیشن‌های معلق اگر ربات آنلاین است
            if self.is_bot_connected and self.pending_notifications:
                logger.info(f"📤 Sending {len(self.pending_notifications)} pending notifications to bot")
                for notif in list(self.pending_notifications):
                    await self.send_to_bot(notif)
                self.pending_notifications.clear()

            logger.info(f"✅ Device {set_code} connected")
            return True
        except Exception as e:
            logger.error(f"❌ Error connecting device {set_code}: {e}")
            return False
        finally:
            async with self._lock:
                self.connecting_devices.discard(set_code)

    async def disconnect_device(self, set_code: str):
        async with self._lock:
            if set_code in self.devices:
                try:
                    await self.devices[set_code].close(code=1000, reason="Disconnect")
                except:
                    pass
                try:
                    del self.devices[set_code]
                except KeyError:
                    pass
            if set_code in self.device_connect_time:
                try:
                    del self.device_connect_time[set_code]
                except KeyError:
                    pass
            self.connecting_devices.discard(set_code)

        logger.info(f"📴 Device {set_code} disconnected")

    async def connect_bot(self, bot_id: str, websocket: WebSocket):
        async with self._lock:
            if bot_id in self.bot_connections:
                try:
                    await self.bot_connections[bot_id].close(code=1000, reason="New connection")
                except:
                    pass
                try:
                    del self.bot_connections[bot_id]
                except KeyError:
                    pass

        await websocket.accept()
        async with self._lock:
            self.bot_connections[bot_id] = websocket
            self.is_bot_connected = True

        logger.info(f"✅ Bot {bot_id} connected")

        if self.pending_notifications:
            logger.info(f"📤 Sending {len(self.pending_notifications)} pending notifications to newly connected bot")
            for notif in list(self.pending_notifications):
                await self.send_to_bot(notif)
            self.pending_notifications.clear()

    async def disconnect_bot(self, bot_id: str):
        async with self._lock:
            if bot_id in self.bot_connections:
                try:
                    del self.bot_connections[bot_id]
                except KeyError:
                    pass
                self.is_bot_connected = len(self.bot_connections) > 0
        logger.info(f"🔌 Bot {bot_id} disconnected")

    async def cleanup_stale_connections(self):
        """پاکسازی اتصالات قدیمی و قطع فیزیکی وب‌سوکت‌های نیمه‌باز (Dead/Half-Open TCP)"""
        now = time.time()
        stale_timeout = 90  # اگر دستگاه بیش از ۹۰ ثانیه هیچ پیامی فرستاده باشد، قطع کانکشن است
        stale_keys = []
        
        for set_code, conn_time in list(self.device_connect_time.items()):
            if now - conn_time > stale_timeout:
                stale_keys.append(set_code)
                
        for key in stale_keys:
            logger.warning(f"⏰ Connection timed out for {key} (heartbeat lost), disconnecting...")
            await self.disconnect_device(key)

manager = ConnectionManager()

# ============================================================
# راه‌اندازی اپلیکیشن با CORS مناسب
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    await manager.init_session()
    async def cleanup_loop():
        try:
            while True:
                await asyncio.sleep(30)  # بررسی ثانیه‌ای جهت پایداری بالا و واکنش سریع به قطعی‌ها
                await manager.cleanup_stale_connections()
        except asyncio.CancelledError:
            pass
    cleanup_task = asyncio.create_task(cleanup_loop())
    logger.info("🚀 WebSocket Real-time Server Started")
    yield
    cleanup_task.cancel()
    await manager.close_session()
    logger.info("🛑 WebSocket Real-time Server Stopped")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# ============================================================
# ★★★ WebSocket Endpoint اصلی ★★★
# ============================================================
@app.websocket("/ws/{set_code}")
async def websocket_endpoint(websocket: WebSocket, set_code: str):
    is_bot = set_code == "bot"
    logger.info(f"🔄 WebSocket connection attempt for {set_code}")

    try:
        if not is_bot:
            success = await manager.connect_device(set_code, websocket)
            if not success:
                return
        else:
            await manager.connect_bot("bot", websocket)

        while True:
            try:
                data = await websocket.receive_json()
                
                # ★★★ به محض دریافت هر پیام معتبر، زمان هارت‌بیت دستگاه را به‌روز کن ★★★
                if not is_bot:
                    manager.device_connect_time[set_code] = time.time()
                    
            except WebSocketDisconnect:
                logger.info(f"🔌 WebSocket disconnected normally for {set_code}")
                break
            except json.JSONDecodeError as je:
                logger.warning(f"⚠️ Invalid JSON from {set_code}: {je}")
                continue
            except Exception as e:
                error_msg = str(e).lower()
                if "websocket is not connected" in error_msg or "accept" in error_msg:
                    logger.warning(f"⚠️ Connection already closed for {set_code}, breaking loop")
                    break
                else:
                    logger.error(f"❌ Error receiving data from {set_code}: {e}")
                    continue

            if not data or 'type' not in data:
                continue

            msg_type = data['type']
            logger.info(f"📥 Received: {msg_type} from {set_code}")

            # ============================================================
            # پردازش پیام‌های دستگاه
            # ============================================================
            if not is_bot:
                if msg_type == 'status':
                    await websocket.send_json({'type': 'status_ack', 'success': True})

                elif msg_type == 'result':
                    # ★★★ دریافت نتیجه و ارسال فوری به ربات (بدون دیتابیس) ★★★
                    command_id = data.get('command_id')
                    success = data.get('success', True)
                    result_data = data.get('data', {})
                    # پاسخ به دستگاه
                    await websocket.send_json({
                        'type': 'result_ack',
                        'command_id': command_id,
                        'success': True
                    })
                    # اگر ربات آنلاین است، نتیجه را مستقیماً بفرست
                    if manager.is_bot_connected:
                        await manager.send_to_bot({
                            'type': 'result',
                            'command_id': command_id,
                            'data': result_data,
                            'set_code': set_code,
                            'success': success
                        })
                        logger.info(f"✅ Result for command {command_id} sent to bot")
                    else:
                        logger.warning(f"⚠️ Bot offline, result for {command_id} lost (not stored)")

                elif msg_type == 'notification':
                    # نوتیفیکیشن را به ربات می‌فرستیم (در صورت آفلاین، کش می‌شود)
                    notif_type = data.get('notification_type', 'general')
                    notif_data = data.get('data', {})
                    await websocket.send_json({'type': 'notification_ack', 'success': True})
                    notif_obj = {
                        'type': 'notification',
                        'data': {
                            'id': int(time.time() * 1000),
                            'set_code': set_code,
                            'type': notif_type,
                            'data': notif_data,
                            'timestamp': datetime.datetime.now().isoformat()
                        }
                    }
                    if manager.is_bot_connected:
                        await manager.send_to_bot(notif_obj)
                        logger.info(f"📬 Notification {notif_type} sent to bot")
                    else:
                        manager.pending_notifications.append(notif_obj)
                        logger.warning(f"⚠️ Bot offline, notification cached ({len(manager.pending_notifications)} pending)")

                elif msg_type == 'command_ack':
                    command_id = data.get('command_id')
                    logger.info(f"✅ Command {command_id} acknowledged by {set_code}")

                elif msg_type == 'ping':
                    await websocket.send_json({"type": "pong", "timestamp": time.time()})
                    logger.debug(f"💓 Pong sent to {set_code}")

                else:
                    await websocket.send_json({'type': 'error', 'message': f'Unknown type: {msg_type}'})

            # ============================================================
            # پردازش پیام‌های ربات
            # ============================================================
            else:
                if msg_type == 'get_device':
                    device_set_code = data.get('set_code')
                    device_data = await manager.api_request(f"device/{device_set_code}", method='GET')
                    device_info = device_data.get('device') if device_data else None
                    if device_info:
                        is_conn = device_set_code in manager.devices
                        device_info['status'] = 'online' if is_conn else 'offline'
                    await websocket.send_json({
                        'type': 'device_info',
                        'set_code': device_set_code,
                        'device': device_info
                    })

                elif msg_type == 'online_devices':
                    # ★★★ اصلاح ریشه‌ای و وب‌سوکت‌محور منطق کاربران آنلاین ★★★
                    online_list = []
                    active_set_codes = list(manager.devices.keys())
                    
                    for set_code in active_set_codes:
                        device_data = await manager.api_request(f"device/{set_code}", method='GET')
                        device_info = device_data.get('device') if device_data else None
                        if device_info:
                            device_info['status'] = 'online'
                            online_list.append(device_info)
                        else:
                            online_list.append({
                                'set_code': set_code,
                                'device_name': 'Unknown',
                                'status': 'online'
                            })
                    await websocket.send_json({
                        'type': 'online_devices',
                        'devices': online_list
                    })

                elif msg_type == 'all_devices':
                    devices_data = await manager.api_request('all_devices', method='GET')
                    all_list = devices_data.get('devices', []) if devices_data else []
                    for d in all_list:
                        is_conn = d.get('set_code') in manager.devices
                        d['status'] = 'online' if is_conn else 'offline'
                    await websocket.send_json({
                        'type': 'all_devices',
                        'devices': all_list
                    })

                elif msg_type == 'stats':
                    # اصلاح منطق آمار: گرفتن کل کاربران از data.php و کم کردن تعداد وب‌ساکت‌های باز برای نمایش آفلاین‌ها
                    all_devices_data = await manager.api_request('all_devices', method='GET')
                    all_list = all_devices_data.get('devices', []) if all_devices_data else []
                    total_users = len(all_list)
                    online_users = len(manager.devices)
                    offline_users = max(0, total_users - online_users)
                    await websocket.send_json({
                        'type': 'stats',
                        'stats': {
                            'total_users': total_users,
                            'online_users': online_users,
                            'offline_users': offline_users
                        }
                    })

                elif msg_type == 'add_command':
                    # ★★★ ارسال مستقیم دستور به دستگاه (بدون دیتابیس) ★★★
                    device_set_code = data.get('set_code')
                    command_type = data.get('command_type')
                    params = data.get('params', {})
                    # استفاده از command_id ارسال شده توسط ربات یا تولید آنی
                    command_id = data.get('command_id') or int(time.time() * 1000)
                    
                    if manager.is_device_online(device_set_code):
                        await manager.send_to_device(device_set_code, {
                            'type': 'command',
                            'data': {
                                'id': command_id,
                                'command_type': command_type,
                                'params': params,
                                'timestamp': datetime.datetime.now().isoformat()
                            }
                        })
                        logger.info(f"📤 Command {command_type} sent to {device_set_code}")
                        await websocket.send_json({
                            'type': 'success',
                            'action': 'add_command',
                            'command_id': command_id,
                            'set_code': device_set_code
                        })
                    else:
                        logger.warning(f"⚠️ Device {device_set_code} offline, command not sent")
                        await websocket.send_json({
                            'type': 'error',
                            'action': 'add_command',
                            'set_code': device_set_code,
                            'message': 'Device is offline'
                        })

                elif msg_type == 'update_nickname':
                    device_set_code = data.get('set_code')
                    nickname = data.get('nickname')
                    result = await manager.api_request('update_nickname', method='POST', data={
                        'setCode': device_set_code,
                        'nickname': nickname
                    })
                    if result and result.get('success'):
                        await websocket.send_json({
                            'type': 'success',
                            'action': 'update_nickname',
                            'set_code': device_set_code
                        })
                    else:
                        await websocket.send_json({
                            'type': 'error',
                            'action': 'update_nickname',
                            'set_code': device_set_code,
                            'message': 'Failed to update nickname'
                        })

                elif msg_type == 'get_result':
                    # در معماری جدید نتایج در دیتابیس نیست
                    await websocket.send_json({
                        'type': 'error',
                        'action': 'get_result',
                        'message': 'Results are not stored in database (real-time only)'
                    })

                elif msg_type == 'delete_command':
                    await websocket.send_json({
                        'type': 'error',
                        'action': 'delete_command',
                        'message': 'Commands are not stored in database (real-time only)'
                    })

                elif msg_type == 'delete_notification':
                    notification_id = data.get('notification_id')
                    removed = False
                    for i, n in enumerate(manager.pending_notifications):
                        if n.get('data', {}).get('id') == notification_id:
                            del manager.pending_notifications[i]
                            removed = True
                            break
                    if removed:
                        await websocket.send_json({
                            'type': 'success',
                            'action': 'delete_notification'
                        })
                    else:
                        await websocket.send_json({
                            'type': 'error',
                            'action': 'delete_notification',
                            'message': 'Notification not found in cache'
                        })

                elif msg_type == 'register':
                    device_data = data.get('data', {})
                    result = await manager.api_request('register', method='POST', data=device_data)
                    if result and result.get('success'):
                        await websocket.send_json({
                            'type': 'success',
                            'action': 'register',
                            'set_code': result.get('set_code')
                        })
                        logger.info(f"✅ New device registered: {result.get('set_code')}")
                    else:
                        await websocket.send_json({
                            'type': 'error',
                            'action': 'register',
                            'message': 'Failed to register device'
                        })

                elif msg_type == 'ping':
                    await websocket.send_json({"type": "pong", "timestamp": time.time()})
                    logger.debug("💓 Pong sent to bot")

                else:
                    await websocket.send_json({'type': 'error', 'message': f'Unknown type: {msg_type}'})

    except WebSocketDisconnect:
        logger.info(f"🔌 {set_code} disconnected (outer)")
    except Exception as e:
        logger.error(f"❌ WebSocket error for {set_code}: {e}")
    finally:
        if is_bot:
            await manager.disconnect_bot("bot")
        else:
            await manager.disconnect_device(set_code)

# ============================================================
# HTTP Endpoints
# ============================================================
@app.get("/health")
async def health_check():
    return {
        "status": "alive",
        "devices_online": len(manager.devices),
        "bot_connected": manager.is_bot_connected,
        "pending_notifications": len(manager.pending_notifications),
        "timestamp": time.time()
    }

@app.get("/")
async def root():
    return {
        "message": "WebSocket Server is running",
        "websocket_endpoint": "/ws/{set_code}",
        "health": "/health"
    }

# ============================================================
# نقطه شروع
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    ws_ping_interval = int(os.getenv("WS_PING_INTERVAL", 20))
    ws_ping_timeout = int(os.getenv("WS_PING_TIMEOUT", 60))

    logger.info(f"🔧 Starting server on port {port}")
    logger.info(f"🔧 WS_PING_INTERVAL={ws_ping_interval}s, WS_PING_TIMEOUT={ws_ping_timeout}s")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        ws_ping_interval=ws_ping_interval,
        ws_ping_timeout=ws_ping_timeout,
        log_level="info",
        access_log=True
    )