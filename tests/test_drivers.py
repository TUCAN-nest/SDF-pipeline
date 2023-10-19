import re
import sqlite3
import pytest
import logging
import json
from operator import add
from functools import reduce
from pathlib import Path
from typing import Callable
from sdf_pipeline import drivers


def regression_consumer(
    molfile: str, get_molfile_id: Callable
) -> drivers.ConsumerResult:
    return drivers.ConsumerResult(
        get_molfile_id(molfile),
        "regression",
        str(len(molfile)),
    )


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
        reference_path=tmp_path / "regression_reference.sqlite",
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


def test_regression_driver(sdf_path, reference_path, caplog):
    caplog.set_level(logging.INFO, logger="sdf_pipeline")
    exit_code = drivers.regression(
        sdf_path=sdf_path,
        reference_path=reference_path,
        consumer_function=regression_consumer,
        get_molfile_id=_get_mcule_id,
        number_of_consumer_processes=2,
    )
    assert exit_code == 1
    assert len(caplog.records) == 1
    log_entry = json.loads(caplog.records[0].message.lstrip("regression test failed:"))
    log_entry.pop("time")
    assert log_entry == {
        "molfile_id": "9261759198",
        "sdf": "mcule_20000.sdf.gz",
        "info": "regression",
        "assertion": "current: '920' != reference: '42'",
    }
