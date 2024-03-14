import re
import sqlite3
import pytest
import logging
import json
import ctypes
import timeit
from operator import add
from functools import reduce
from pathlib import Path
from typing import Callable
from functools import partial
from concurrent.futures.process import BrokenProcessPool
from sdf_pipeline import drivers, core


def regression_consumer(
    molfile: str, get_molfile_id: Callable
) -> drivers.ConsumerResult:

    return drivers.ConsumerResult(
        molfile_id=get_molfile_id(molfile),
        info={"consumer": "regression"},
        result={
            "molfile_length": (
                42 if get_molfile_id(molfile) == "9261759198" else len(molfile)
            )
        },
    )


def busy_consumer(molfile: str, get_molfile_id: Callable) -> drivers.ConsumerResult:
    n = 0
    for i in range(100):
        n += sum([len(line) ** i for line in molfile.split("\n")])

    return drivers.ConsumerResult(
        molfile_id=get_molfile_id(molfile),
        info={"consumer": "busy_consumer"},
        result={"large_result": n},
    )


def segfaulting_consumer(
    molfile: str, get_molfile_id: Callable
) -> drivers.ConsumerResult:
    ctypes.string_at(0)


def raising_consumer(molfile: str, get_molfile_id: Callable) -> drivers.ConsumerResult:
    1 / 0


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
        assert (
            reduce(
                add,
                [int(json.loads(result[0])["molfile_length"]) for result in results],
            )
            == 31062992
        )


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
    assert log_entry["molfile_id"] == "9261759198"
    assert log_entry["sdf"] == "mcule_20000.sdf.gz"
    assert log_entry["info"] == {"consumer": "regression", "parameters": ""}
    assert log_entry["diff"] == {
        "current": '{"molfile_length": 42}',
        "reference": '{"molfile_length": 926}',
    }


def test_core_raises_on_exception(sdf_path, caplog):
    caplog.set_level(logging.ERROR, logger="sdf_pipeline")
    with pytest.raises(ZeroDivisionError):
        for _ in core.run(
            sdf_path=str(sdf_path),
            consumer_function=partial(raising_consumer, get_molfile_id=_get_mcule_id),
            number_of_consumer_processes=2,
        ):
            pass
    assert (
        caplog.records[-1].message == f"could not process {sdf_path}: division by zero"
    )


def test_core_raises_on_segfault(sdf_path, caplog):
    caplog.set_level(logging.ERROR, logger="sdf_pipeline")
    with pytest.raises(BrokenProcessPool):
        for _ in core.run(
            sdf_path=str(sdf_path),
            consumer_function=partial(
                segfaulting_consumer, get_molfile_id=_get_mcule_id
            ),
            number_of_consumer_processes=2,
        ):
            pass
    # See comment in `core.run` for why there's two alternative messages.
    assert caplog.records[-1].message in [
        f"could not process {sdf_path}: A process in the process pool was terminated abruptly while the future was running or pending.",
        f"could not process {sdf_path}: A child process terminated abruptly, the process pool is not usable anymore",
    ]


@pytest.mark.skip
def test_performance(sdf_path):
    def run_core(n_processes):
        def _run_core():
            for _ in core.run(
                sdf_path=str(sdf_path),
                consumer_function=partial(busy_consumer, get_molfile_id=_get_mcule_id),
                number_of_consumer_processes=n_processes,
            ):
                pass

        return _run_core

    previous_execution_time = 0
    for n in [1, 2, 4, 8, 16]:
        execution_time = timeit.Timer(run_core(n)).timeit(1)
        print(f"{n} processes finished in {execution_time} seconds.")

        if previous_execution_time:
            assert execution_time < previous_execution_time

        previous_execution_time = execution_time
