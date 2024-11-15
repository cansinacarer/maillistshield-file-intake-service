import requests
from decouple import config

# Uptime monitor address
UPTIME_MONITOR = config("UPTIME_MONITOR")


# Send a heartbeat the the uptime monitor
def ping_uptime_monitor():
    try:
        requests.get(UPTIME_MONITOR)
    except:
        pass
