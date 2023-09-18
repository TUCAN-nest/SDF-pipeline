import re
import sqlite3
import pytest
from operator import add
from functools import reduce
from pathlib import Path
from sdf_pipeline import drivers
from consumers import regression_consumer


@pytest.fixture
def sdf_path():
    return Path(__file__).parent.absolute().joinpath("data/mcule_20000.sdf.gz")


@pytest.fixture
def reference_path():
    return (
        Path(__file__)
        .parent.absolute()
        .joinpath("data/mcule_20000_regression_reference.sqlite")
    )


def _get_mcule_id(molfile: str) -> str:
    molfile_id_pattern = re.compile(r"<Mcule_ID>(.*?)>", re.DOTALL)
    molfile_id_match = molfile_id_pattern.search(molfile)
    molfile_id = molfile_id_match.group(1).strip() if molfile_id_match else ""
    return molfile_id


def test_regression_reference_driver(sdf_path, tmp_path):
    exit_code = drivers.regression_reference(
        sdf_path=sdf_path,
        log_path=tmp_path / "regression_reference.sqlite",
        consumer_function=regression_consumer,
        get_molfile_id=_get_mcule_id,
        number_of_consumer_processes=2,
    )
    assert exit_code == 0
    with sqlite3.connect(tmp_path / "regression_reference.sqlite") as db:
        results = db.execute(
            "SELECT result FROM results ",
        ).fetchall()
        assert len(results) == 20000
        assert reduce(add, [int(result[0]) for result in results]) == 30943876


def test_regression_driver(sdf_path, reference_path, tmp_path):
    exit_code = drivers.regression(
        sdf_path=sdf_path,
        log_path=tmp_path / "regression.sqlite",
        reference_path=reference_path,
        consumer_function=regression_consumer,
        get_molfile_id=_get_mcule_id,
        number_of_consumer_processes=2,
    )

    assert exit_code == 1
    with sqlite3.connect(tmp_path / "regression.sqlite") as db:
        failed_id, failed_result = db.execute(
            "SELECT molfile_id, result FROM results WHERE result != 'passed'",
        ).fetchone()
        assert failed_id == "9261759198"
        assert failed_result == "current: '920' != reference: '42'"
