import sqlite3
from datetime import datetime
from dataclasses import dataclass, asdict


@dataclass
class ConsumerResult:
    molfile_id: str = ""
    time: str = ""
    info: str = ""
    result: str = ""


def get_current_time() -> str:
    return datetime.now().isoformat(timespec="seconds")


def log_result(log_db: sqlite3.Connection, result: ConsumerResult) -> None:
    log_db.execute(
        "INSERT INTO results VALUES (:molfile_id, :time, :info, :result)",
        asdict(result),
    )


def create_results_table(db: sqlite3.Connection) -> None:
    db.execute("CREATE TABLE results (molfile_id UNIQUE, time, info, result)")
