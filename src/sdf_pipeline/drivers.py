import sqlite3
from typing import Callable
from functools import partial
from sdf_pipeline import core, utils
from pathlib import Path
from dataclasses import astuple


def invariance(
    sdf_path: str,
    log_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
) -> int:
    with sqlite3.connect(log_path) as log_db:
        utils.create_results_table(log_db)

        exit_code = 0

        for consumer_result in core.run(
            sdf_path=sdf_path,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        ):
            molfile_id, time, info, assertion = astuple(consumer_result)
            if assertion != "passed":
                exit_code = 1
                print(
                    f"{time}: invariance test failed for molfile {molfile_id} from {Path(sdf_path).name} (computed with {info}): {assertion}."
                )
                utils.log_result(log_db, consumer_result)

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
        exit_code = 0
        processed_molfile_ids = set()

        for consumer_result in core.run(
            sdf_path=sdf_path,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        ):
            molfile_id, time, info, current_result = astuple(consumer_result)
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

            assertion = "passed"
            if current_result != reference_result:
                exit_code = 1
                assertion = (
                    f"current: '{current_result}' != reference: '{reference_result}'"
                )
                print(
                    f"{time}: regression test failed for molfile {molfile_id} from {Path(sdf_path).name} (computed with {info}): {assertion}."
                )
                utils.log_result(
                    log_db,
                    utils.ConsumerResult(
                        molfile_id=molfile_id,
                        time=time,
                        info=info,
                        result=assertion,
                    ),
                )

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

        for consumer_result in core.run(
            sdf_path=sdf_path,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        ):
            utils.log_result(log_db, consumer_result)

        log_db.execute(
            "CREATE INDEX IF NOT EXISTS molfile_id_index ON results (molfile_id)"
        )  # crucial, reduces look-up speed by orders of magnitude

    return 0
