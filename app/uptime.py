import requests
from app.config import UPTIME_MONITOR


# Send a heartbeat the the uptime monitor
def ping_uptime_monitor():
    try:
        requests.get(UPTIME_MONITOR)
    except:
        pass
