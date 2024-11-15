import time

from app.uptime import ping_uptime_monitor


def main():
    # Main loop
    while True:
        start_time = time.time()
        ping_uptime_monitor()
        end_time = time.time()
        elapsed_time = end_time - start_time
        sleep_time = 60 - elapsed_time
        if sleep_time > 0:
            time.sleep(sleep_time)
