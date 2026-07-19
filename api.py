from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Optional
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

# ★★★ تنظیمات پایداری ★★★
PING_INTERVAL = 20         # هر ۲۰ ثانیه ping ارسال شود
PING_TIMEOUT = 30          # اگر ۳۰ ثانیه پاسخ نداد، قطع کن
MAX_NOTIFICATIONS_CACHE = 200
RECONNECT_DELAY = 5

# ============================================================
# کلاس مدیریت اتصالات و کش
# ============================================================
class ConnectionManager:
    def __init__(self):
        self.devices: Dict[str, WebSocket] = {}
        self.device_connect_time: Dict[str, float] = {}
        self.bot_connections: Dict[str, WebSocket] = {}
        self.is_bot_connected = False
        self.api_session: Optional[aiohttp.ClientSession] = None

        # ★★★ کش برای نوتیفیکیشن‌های ارسال‌نشده (زمانی که ربات آفلاین است) ★★★
        self.pending_notifications: deque = deque(maxlen=MAX_NOTIFICATIONS_CACHE)

        # ★★★ جلوگیری از اتصالات همزمان ★★★
        self.connecting_devices: set = set()

    async def init_session(self):
        if self.api_session is None:
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            self.api_session = aiohttp.ClientSession(timeout=timeout)

    async def close_session(self):
        if self.api_session:
            await self.api_session.close()
            self.api_session = None

    async def api_request(self, endpoint: str, method: str = 'GET', data: dict = None, retries: int = 2) -> Optional[dict]:
        url = f"{PHP_API_URL}/{endpoint.lstrip('/')}"
        headers = {"Content-Type": "application/json"}

        for attempt in range(retries + 1):
            try:
                if method.upper() == 'GET':
                    async with self.api_session.get(url, headers=headers) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        logger.error(f"API GET error: {resp.status} - {await resp.text()[:100]}")
                elif method.upper() == 'POST':
                    async with self.api_session.post(url, json=data, headers=headers) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        logger.error(f"API POST error: {resp.status} - {await resp.text()[:100]}")
                elif method.upper() == 'DELETE':
                    async with self.api_session.delete(url, headers=headers) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        logger.error(f"API DELETE error: {resp.status}")
            except aiohttp.ClientError as e:
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
            except Exception:
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
        # جلوگیری از اتصال همزمان چندگانه
        if set_code in self.connecting_devices:
            logger.warning(f"⚠️ Device {set_code} already connecting, closing duplicate")
            await websocket.close(code=1000, reason="Duplicate connection")
            return False

        # بستن اتصال قبلی اگر وجود داشته باشد
        if set_code in self.devices:
            logger.warning(f"⚠️ Device {set_code} already connected, closing old connection")
            try:
                await self.devices[set_code].close(code=1000, reason="New connection")
            except:
                pass
            del self.devices[set_code]

        self.connecting_devices.add(set_code)
        try:
            await websocket.accept()
            self.devices[set_code] = websocket
            self.device_connect_time[set_code] = time.time()

            # به‌روزرسانی وضعیت در دیتابیس
            await self.api_request('status', method='POST', data={
                'setCode': set_code,
                'battery': None,
                'simInfo': None
            })

            # ارسال دستورات معلق
            commands_data = await self.api_request(f"commands/{set_code}", method='GET')
            if commands_data and commands_data.get('commands'):
                for cmd in commands_data['commands']:
                    try:
                        await websocket.send_json({
                            'type': 'command',
                            'data': {
                                'id': cmd['id'],
                                'command_type': cmd['command_type'],
                                'params': cmd['params'],
                                'timestamp': cmd.get('timestamp', '')
                            }
                        })
                    except Exception:
                        pass

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
            self.connecting_devices.discard(set_code)

    async def disconnect_device(self, set_code: str):
        if set_code in self.devices:
            try:
                await self.devices[set_code].close(code=1000, reason="Disconnect")
            except:
                pass
            del self.devices[set_code]
        if set_code in self.device_connect_time:
            del self.device_connect_time[set_code]

        await self.api_request('status', method='POST', data={
            'setCode': set_code,
            'battery': None,
            'simInfo': None
        })
        logger.info(f"📴 Device {set_code} disconnected")

    async def connect_bot(self, bot_id: str, websocket: WebSocket):
        await websocket.accept()
        self.bot_connections[bot_id] = websocket
        self.is_bot_connected = True
        logger.info(f"✅ Bot {bot_id} connected")

        # ارسال نوتیفیکیشن‌های معلق به ربات تازه متصل‌شده
        if self.pending_notifications:
            logger.info(f"📤 Sending {len(self.pending_notifications)} pending notifications to newly connected bot")
            for notif in list(self.pending_notifications):
                await self.send_to_bot(notif)
            self.pending_notifications.clear()

    async def disconnect_bot(self, bot_id: str):
        if bot_id in self.bot_connections:
            del self.bot_connections[bot_id]
            self.is_bot_connected = len(self.bot_connections) > 0
            logger.info(f"🔌 Bot {bot_id} disconnected")

    def cleanup_stale_connections(self):
        """پاکسازی اتصالات قدیمی (اختیاری)"""
        now = time.time()
        stale_timeout = 300  # 5 دقیقه
        for set_code, conn_time in list(self.device_connect_time.items()):
            if now - conn_time > stale_timeout and set_code not in self.devices:
                # اگر در دیکشنری devices نیست ولی در time هست، حذفش کن
                del self.device_connect_time[set_code]

# ============================================================
# راه‌اندازی اپلیکیشن
# ============================================================
manager = ConnectionManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    await manager.init_session()
    # شروع تسک پاکسازی
    async def cleanup_loop():
        while True:
            await asyncio.sleep(60)
            manager.cleanup_stale_connections()
    asyncio.create_task(cleanup_loop())
    logger.info("🚀 WebSocket Real-time Server Started")
    yield
    await manager.close_session()
    logger.info("🛑 WebSocket Real-time Server Stopped")

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# ★★★ WebSocket Endpoint اصلی ★★★
# ============================================================
@app.websocket("/ws/{set_code}")
async def websocket_endpoint(websocket: WebSocket, set_code: str):
    is_bot = set_code == "bot"

    if not is_bot:
        # بررسی وجود دستگاه در دیتابیس
        device_data = await manager.api_request(f"device/{set_code}", method='GET')
        if not device_data or not device_data.get('device'):
            await websocket.close(code=1008, reason="Device not found")
            return

        # اتصال دستگاه (با مدیریت duplicate)
        connected = await manager.connect_device(set_code, websocket)
        if not connected:
            return
    else:
        await manager.connect_bot("bot", websocket)

    try:
        while True:
            # ★★★ مدیریت خطای JSON با try-except جداگانه ★★★
            try:
                data = await websocket.receive_json()
            except json.JSONDecodeError as je:
                logger.warning(f"⚠️ Invalid JSON received: {je}")
                continue
            except WebSocketDisconnect:
                raise
            except Exception as e:
                logger.error(f"❌ Error receiving data: {e}")
                continue

            msg_type = data.get('type')
            if not msg_type:
                logger.warning("⚠️ Message without type received")
                continue

            logger.info(f"📥 Received: {msg_type} from {'bot' if is_bot else set_code}")

            # ============================================================
            # پردازش پیام‌های دستگاه
            # ============================================================
            if not is_bot:
                if msg_type == 'status':
                    battery = data.get('battery')
                    sim_info = data.get('simInfo')
                    await manager.api_request('status', method='POST', data={
                        'setCode': set_code,
                        'battery': battery,
                        'simInfo': sim_info
                    })
                    await websocket.send_json({'type': 'status_ack', 'success': True})

                    # ارسال دستورات جدید
                    commands_data = await manager.api_request(f"commands/{set_code}", method='GET')
                    if commands_data and commands_data.get('commands'):
                        for cmd in commands_data['commands']:
                            await websocket.send_json({
                                'type': 'command',
                                'data': {
                                    'id': cmd['id'],
                                    'command_type': cmd['command_type'],
                                    'params': cmd['params'],
                                    'timestamp': cmd.get('sent_at', '')
                                }
                            })

                elif msg_type == 'result':
                    command_id = data.get('command_id')
                    success = data.get('success', True)
                    result_data = data.get('data', {})

                    await manager.api_request('result', method='POST', data={
                        'commandId': command_id,
                        'setCode': set_code,
                        'success': success,
                        'data': result_data
                    })

                    await websocket.send_json({
                        'type': 'result_ack',
                        'command_id': command_id,
                        'success': True
                    })

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
                        logger.warning(f"⚠️ Bot offline, result for {command_id} stored in DB only")

                elif msg_type == 'notification':
                    notif_type = data.get('notification_type', 'general')
                    notif_data = data.get('data', {})

                    await websocket.send_json({
                        'type': 'notification_ack',
                        'success': True
                    })

                    # ساخت نوتیفیکیشن
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
                        logger.info(f"📬 Notification {notif_type} from {set_code} sent to bot")
                    else:
                        # ★★★ کش کردن نوتیفیکیشن ★★★
                        manager.pending_notifications.append(notif_obj)
                        logger.warning(f"⚠️ Bot offline, notification {notif_type} from {set_code} cached ({len(manager.pending_notifications)} pending)")

                elif msg_type == 'command_ack':
                    command_id = data.get('command_id')
                    await manager.api_request('update_command', method='POST', data={
                        'commandId': command_id,
                        'status': 'done'
                    })
                    logger.info(f"✅ Command {command_id} acknowledged by {set_code}")

                else:
                    await websocket.send_json({
                        'type': 'error',
                        'message': f'Unknown message type: {msg_type}'
                    })

            # ============================================================
            # پردازش پیام‌های ربات
            # ============================================================
            else:
                if msg_type == 'get_device':
                    device_set_code = data.get('set_code')
                    device_data = await manager.api_request(f"device/{device_set_code}", method='GET')
                    await websocket.send_json({
                        'type': 'device_info',
                        'set_code': device_set_code,
                        'device': device_data.get('device') if device_data else None
                    })

                elif msg_type == 'online_devices':
                    devices_data = await manager.api_request('online_devices', method='GET')
                    await websocket.send_json({
                        'type': 'online_devices',
                        'devices': devices_data.get('devices', []) if devices_data else []
                    })

                elif msg_type == 'all_devices':
                    devices_data = await manager.api_request('all_devices', method='GET')
                    await websocket.send_json({
                        'type': 'all_devices',
                        'devices': devices_data.get('devices', []) if devices_data else []
                    })

                elif msg_type == 'stats':
                    stats_data = await manager.api_request('stats', method='GET')
                    await websocket.send_json({
                        'type': 'stats',
                        'stats': stats_data if stats_data else {
                            'total_users': 0,
                            'online_users': 0,
                            'offline_users': 0
                        }
                    })

                elif msg_type == 'add_command':
                    device_set_code = data.get('set_code')
                    command_type = data.get('command_type')
                    params = data.get('params', {})

                    result = await manager.api_request('add_command', method='POST', data={
                        'setCode': device_set_code,
                        'command_type': command_type,
                        'params': params
                    })

                    if result and result.get('success'):
                        command_id = result.get('command_id')

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
                        else:
                            logger.warning(f"⚠️ Device {device_set_code} offline, command stored in DB")

                        await websocket.send_json({
                            'type': 'success',
                            'action': 'add_command',
                            'command_id': command_id
                        })
                    else:
                        await websocket.send_json({
                            'type': 'error',
                            'message': 'Failed to add command'
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
                            'action': 'update_nickname'
                        })
                    else:
                        await websocket.send_json({
                            'type': 'error',
                            'message': 'Failed to update nickname'
                        })

                elif msg_type == 'get_result':
                    command_id = data.get('command_id')
                    result_data = await manager.api_request(f"result/{command_id}", method='GET')
                    await websocket.send_json({
                        'type': 'result',
                        'command_id': command_id,
                        'data': result_data.get('result') if result_data else None
                    })

                elif msg_type == 'delete_command':
                    command_id = data.get('command_id')
                    result = await manager.api_request(f"command/{command_id}", method='DELETE')
                    if result and result.get('success'):
                        await websocket.send_json({
                            'type': 'success',
                            'action': 'delete_command'
                        })
                    else:
                        await websocket.send_json({
                            'type': 'error',
                            'message': 'Failed to delete command'
                        })

                elif msg_type == 'register':
                    # اگر خواستید ثبت از طریق WebSocket انجام شود (اختیاری)
                    device_data = data.get('data', {})
                    result = await manager.api_request('register', method='POST', data=device_data)
                    if result and result.get('success'):
                        await websocket.send_json({
                            'type': 'success',
                            'action': 'register',
                            'set_code': result.get('set_code')
                        })
                        logger.info(f"✅ New device registered via bot: {result.get('set_code')}")
                    else:
                        await websocket.send_json({
                            'type': 'error',
                            'message': 'Failed to register device'
                        })

                else:
                    await websocket.send_json({
                        'type': 'error',
                        'message': f'Unknown message type: {msg_type}'
                    })

    except WebSocketDisconnect:
        logger.info(f"🔌 {'Bot' if is_bot else 'Device ' + set_code} disconnected")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {e}")
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

@app.get("/pending_notifications")
async def get_pending_notifications():
    """برای دیباگ: نمایش نوتیفیکیشن‌های معلق"""
    return {"pending": list(manager.pending_notifications)}

# ============================================================
# نقطه شروع
# ============================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)