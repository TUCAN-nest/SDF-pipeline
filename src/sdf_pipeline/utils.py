import sqlite3
from datetime import datetime
from dataclasses import dataclass, astuple


@dataclass
class ConsumerResult:
    molfile_id: str = ""
    time: str = ""
    info: str = ""
    result: str = ""


def get_current_time() -> str:
    return datetime.now().isoformat(timespec="seconds")


def log_result(log_db: sqlite3.Connection, result: ConsumerResult) -> None:
    log_db.execute("INSERT INTO results VALUES (?, ?, ?, ?)", astuple(result))
