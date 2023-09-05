import sqlite3
from typing import Callable
from functools import partial
from sdf_pipeline import core, utils
from unittest import TestCase


def _create_results_table(db: sqlite3.Connection) -> None:
    db.execute("CREATE TABLE results (consumer, time, molfile_id UNIQUE, result)")


def invariance(
    sdf_path: str,
    log_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
) -> int:
    with sqlite3.connect(log_path) as log_db:
        _create_results_table(log_db)

        core.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        )

        exit_code = 0
        for time, molfile_id, assertion in log_db.execute(
            "SELECT time, molfile_id, result FROM results"
        ):
            if assertion != "passed":
                exit_code = 1
                print(
                    f"{time}: invariance test failed for molfile {molfile_id}: {assertion}."
                )

    return exit_code


def regression(
    sdf_path: str,
    log_path: str,
    reference_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
) -> int:
    with (
        sqlite3.connect(":memory:") as intermediate_log_db,
        sqlite3.connect(log_path) as log_db,
        sqlite3.connect(reference_path) as reference_db,
    ):
        _create_results_table(intermediate_log_db)
        _create_results_table(log_db)

        core.run(
            sdf_path=sdf_path,
            log_db=intermediate_log_db,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        )

        intermediate_log_db.execute(
            "CREATE INDEX IF NOT EXISTS molfile_id_index ON results (molfile_id)"
        )  # crucial, reduces look-up speed by orders of magnitude

        exit_code = 0
        for molfile_id, reference_result in reference_db.execute(
            "SELECT molfile_id, result FROM results"
        ):
            query_result = intermediate_log_db.execute(
                "SELECT time, result FROM results WHERE molfile_id = ?",
                (molfile_id,),
            ).fetchall()
            assert query_result, f"Couldn't find molfile ID {molfile_id}."
            assert len(query_result) == 1, f"Molfile ID {molfile_id} is not unique."

            time, current_result = query_result[0]

            assertion = "passed"
            try:
                TestCase().assertEqual(current_result, reference_result)
            except AssertionError as exception:
                exit_code = 1
                assertion = str(exception)
                print(
                    f"{time}: regression test failed for molfile {molfile_id}: {assertion}."
                )

            utils.log_result(
                log_db,
                utils.ConsumerResult(
                    "regression",
                    time,
                    molfile_id,
                    assertion,
                ),
            )

    return exit_code


def regression_reference(
    sdf_path: str,
    log_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
) -> int:
    with sqlite3.connect(log_path) as log_db:
        _create_results_table(log_db)

        core.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        )

    return 0
