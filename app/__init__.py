import time

from app.uptime import ping_uptime_monitor


def main():
    # Main loop
    while True:
        time.sleep(60)
        ping_uptime_monitor()
