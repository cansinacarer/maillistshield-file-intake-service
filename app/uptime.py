import requests
from app.config import UPTIME_MONITOR
from app.logging import logger


# Send a heartbeat the the uptime monitor
def ping_uptime_monitor():
    try:
        requests.get(UPTIME_MONITOR)
    except Exception as e:
        logger.error(f"Error while sending heartbeat to uptime monitor: {e}")
