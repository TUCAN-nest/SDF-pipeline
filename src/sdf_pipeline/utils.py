from datetime import datetime


def get_current_time():
    return datetime.now().isoformat(timespec="seconds")
