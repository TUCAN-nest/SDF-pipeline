from datetime import datetime
from typing import NamedTuple


class ConsumerResult(NamedTuple):
    consumer: str
    time: str
    molfile_id: str
    result: str


def get_current_time():
    return datetime.now().isoformat(timespec="seconds")
