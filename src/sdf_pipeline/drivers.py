"""
From https://docs.python.org/3/library/sqlite3.html#how-to-use-the-connection-context-manager:
'If the body of the with statement finishes without exceptions, the transaction is committed.
If this commit fails, or if the body of the with statement raises an uncaught exception, the transaction is rolled back.'

Contrary to the conventional behavior of context managers, the connection is not closed upon leaving the `with` block:
'The context manager neither implicitly opens a new transaction nor closes the connection.'
See also https://blog.rtwilson.com/a-python-sqlite3-context-manager-gotcha/.

"""

import sqlite3
import json
from typing import Callable
from functools import partial
from pathlib import Path
from dataclasses import astuple
from datetime import datetime
from dataclasses import dataclass, asdict, field
from sdf_pipeline import core, logger


@dataclass
class ConsumerResult:
    molfile_id: str
    info: str
    result: str
    time: str = field(
        default_factory=lambda: datetime.now().isoformat(timespec="seconds")
    )


def regression(
    sdf_path: str,
    reference_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
) -> int:
    with sqlite3.connect(reference_path) as reference_db:
        exit_code = 0
        processed_molfile_ids = set()

        for consumer_result in core.run(
            sdf_path=sdf_path,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        ):
            molfile_id, info, current_result, time = astuple(consumer_result)
            assert (
                molfile_id not in processed_molfile_ids
            ), f"Molfile ID {molfile_id} has been processed multiple times."
            processed_molfile_ids.add(molfile_id)

            reference_query = reference_db.execute(
                "SELECT result FROM results WHERE molfile_id = ?",
                (molfile_id,),
            ).fetchone()
            assert (
                reference_query
            ), f"Couldn't find molfile ID {molfile_id} in reference."
            reference_result = reference_query[0]

            if current_result != reference_result:
                exit_code = 1
                diff = json.dumps(
                    {"current": current_result, "reference": reference_result}
                )
                log_entry = json.dumps(
                    {
                        "time": time,
                        "molfile_id": molfile_id,
                        "sdf": Path(sdf_path).name,
                        "info": info,
                        "diff": diff,
                    }
                )
                logger.info(f"regression test failed:{log_entry}")

        unprocessed_molfile_ids = (
            set(
                molfile_id[0]
                for molfile_id in reference_db.execute(
                    "SELECT molfile_id FROM results"
                ).fetchall()
            )
            - processed_molfile_ids
        )

        assert (
            not unprocessed_molfile_ids
        ), f"Reference contains molfile IDs that haven't been processed: {unprocessed_molfile_ids}."

    reference_db.close()

    return exit_code


def regression_reference(
    sdf_path: str,
    reference_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
) -> int:
    with sqlite3.connect(reference_path) as reference_db:
        reference_db.execute(
            "CREATE TABLE IF NOT EXISTS results (molfile_id UNIQUE, time, info, result)"
        )

        for consumer_result in core.run(
            sdf_path=sdf_path,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        ):
            reference_db.execute(
                "INSERT INTO results VALUES (:molfile_id, :time, :info, :result)",
                asdict(consumer_result),
            )

        reference_db.execute(
            "CREATE INDEX IF NOT EXISTS molfile_id_index ON results (molfile_id)"
        )  # crucial, reduces look-up speed by orders of magnitude

    reference_db.close()

    return 0
