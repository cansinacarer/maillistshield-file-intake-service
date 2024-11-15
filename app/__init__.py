import time

from app.uptime import ping_uptime_monitor


def main():
    # Main loop
    while True:
        time.sleep(30)
        ping_uptime_monitor()
