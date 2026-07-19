# config.py
# ============================================================
# تنظیمات ساده ربات و سرور
# ============================================================

import os

# توکن ربات تلگرام
BOT_TOKEN = "8885136415:AAE1JwRZajqfSeU3yzl01JIwbyMQUecDV24"

# پورت سرور (از متغیر محیطی یا پیش‌فرض 8000)
PORT = int(os.getenv("PORT", 8000))

# آدرس WebSocket سرور (برای اتصال ربات به api.py در همان کانتینر)
WEBSOCKET_URL = f"ws://localhost:{PORT}/ws/bot"

# آدرس HTTP api.py (برای Health Check و غیره)
HTTP_API_URL = f"http://localhost:{PORT}"

# آیدی گروه تلگرام
GROUP_CHAT_ID = -1003776813159

# آدرس data.php (برای مدیریت یوزرها)
PHP_API_URL = "http://egzozereza.ir/data.php"