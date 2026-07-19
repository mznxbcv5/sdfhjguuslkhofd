# main.py
# ============================================================
# نقطه ورود اصلی پروژه - اجرای همزمان api.py و bot.py
# ============================================================

import threading
import uvicorn
import os
import sys
import time
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("main")

def run_api():
    """اجرای سرور WebSocket (api.py) با uvicorn"""
    try:
        from api import app
        port = int(os.getenv("PORT", 8000))
        host = os.getenv("HOST", "0.0.0.0")
        logger.info(f"🚀 Starting WebSocket Server on port {port}")
        uvicorn.run(
            app,
            host=host,
            port=port,
            log_level="info",
            access_log=True
        )
    except Exception as e:
        logger.error(f"❌ Error running API server: {e}")
        sys.exit(1)

def run_bot():
    """اجرای ربات تلگرام (bot.py)"""
    try:
        from bot import run_bot as bot_main
        logger.info("🤖 Starting Telegram Bot (bot.py)")
        bot_main()
    except Exception as e:
        logger.error(f"❌ Error running bot: {e}")
        sys.exit(1)

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🔄 Starting CEPH Control Panel")
    logger.info("=" * 60)

    bot_thread = threading.Thread(target=run_bot, daemon=True)
    bot_thread.start()
    logger.info("✅ Bot thread started")

    time.sleep(1)

    logger.info("✅ Starting API server...")
    run_api()

    bot_thread.join()