import sqlite3
from typing import Callable
from functools import partial
from sdf_pipeline import core, utils
from pathlib import Path


def invariance(
    sdf_path: str,
    log_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
) -> int:
    with sqlite3.connect(log_path) as log_db:
        utils.create_results_table(log_db)

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
        sqlite3.connect(log_path) as log_db,
        sqlite3.connect(reference_path) as reference_db,
    ):
        utils.create_results_table(log_db)

        core.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        )

        log_db.execute(
            "CREATE INDEX IF NOT EXISTS molfile_id_index ON results (molfile_id)"
        )  # crucial, reduces look-up speed by orders of magnitude

        exit_code = 0
        for molfile_id, reference_result in reference_db.execute(
            "SELECT molfile_id, result FROM results"
        ):
            query_result = log_db.execute(
                "SELECT time, info, result FROM results WHERE molfile_id = ?",
                (molfile_id,),
            ).fetchall()
            assert query_result, f"Couldn't find molfile ID {molfile_id}."
            assert len(query_result) == 1, f"Molfile ID {molfile_id} is not unique."

            time, info, current_result = query_result[0]

            assertion = "passed"
            if current_result != reference_result:
                exit_code = 1
                assertion = (
                    f"current: '{current_result}' != reference: '{reference_result}'"
                )
                print(
                    f"{time}: regression test failed for molfile {molfile_id} from {Path(sdf_path).name} (computed with {info}): {assertion}."
                )

            log_db.execute(
                "UPDATE results SET result = ? WHERE molfile_id = ?",
                (assertion, molfile_id),
            )

        log_db.execute(
            "DROP INDEX molfile_id_index"
        )  # drop index to decrease file size

    return exit_code


def regression_reference(
    sdf_path: str,
    log_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
) -> int:
    with sqlite3.connect(log_path) as log_db:
        utils.create_results_table(log_db)

        core.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        )

    return 0
