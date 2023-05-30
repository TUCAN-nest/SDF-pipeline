import sqlite3
import argparse
from typing import Callable
from functools import partial
from sdf_pipeline import core, utils
from unittest import TestCase


def _create_results_table(db: sqlite3.Connection):
    db.execute(
        "CREATE TABLE results (consumer, time, molfile_id UNIQUE, result)"
    )


def parse_cli_args() -> argparse.Namespace:
    """
    Parse driver-related arguments from command line.

    Examples:
    python -m <package.module> --help
    python -m <package.module> regression --help
    python -m <package.module> invariance --help
    python -m <package.module> invariance --result-destination where/to/save/results.sqlite
    python -m <package.module> regression --compute-reference-result --result-destination where/to/save/reference-results.sqlite
    python -m <package.module> regression --reference-result path/to/reference.sqlite --result-destination where/to/save/results.sqlite
    """
    parser = argparse.ArgumentParser(description="Run tests against SDF.")
    subparsers = parser.add_subparsers(
        required=True, dest="test_type", title="test-type"
    )

    result_destination_args = {
        "default": ":memory:",
        "metavar": "RESULT_DESTINATION",
        "help": "Save results to this path. If not specified, results will be held in-memory for the duration of the current run.",
    }

    invariance_parser = subparsers.add_parser("invariance")
    invariance_parser.add_argument("--result-destination", **result_destination_args)

    regression_parser = subparsers.add_parser("regression")
    regression_parser.add_argument("--result-destination", **result_destination_args)
    group = regression_parser.add_mutually_exclusive_group()
    group.add_argument(
        "--reference-result",
        metavar="REFERENCE_RESULT",
        help="Path to reference results. The current run will be compared against those results.",
    )
    group.add_argument(  # boolean flag, defaults to False
        "--compute-reference-result",
        action="store_true",
        help="Compute reference results against which subsequent runs can be compared.",
    )

    return parser.parse_args()


def invariance(
    sdf_path: str,
    log_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
):
    with sqlite3.connect(log_path) as log_db:
        _create_results_table(log_db)

        core.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        )

        for time, molfile_id, assertion in log_db.execute(
            "SELECT time, molfile_id, result FROM results"
        ):
            if assertion != "passed":
                print(
                    f"{time}: invariance test failed for molfile {molfile_id}: {assertion}."
                )


def regression(
    sdf_path: str,
    log_path: str,
    reference_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
):
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
                assertion = str(exception)
                print(
                    f"{time}: regression test failed for molfile {molfile_id}: {assertion}."
                )

            log_db.execute(
                "INSERT INTO results VALUES (?, ?, ?, ?)",
                utils.ConsumerResult(
                    "regression",
                    time,
                    molfile_id,
                    assertion,
                ),
            )


def regression_reference(
    sdf_path: str,
    log_path: str,
    consumer_function: Callable,
    get_molfile_id: Callable,
    number_of_consumer_processes: int = 8,
):
    with sqlite3.connect(log_path) as log_db:
        _create_results_table(log_db)

        core.run(
            sdf_path=sdf_path,
            log_db=log_db,
            consumer_function=partial(consumer_function, get_molfile_id=get_molfile_id),
            number_of_consumer_processes=number_of_consumer_processes,
        )
