import multiprocessing
import gzip
import sqlite3
from typing import Callable, Iterator
from sdf_pipeline import utils


def _read_molfiles_from_zipped_sdf(sdf_path: str) -> Iterator[str]:
    """Generator yielding molfiles from gzipped SDF file.
    (G)unzips the SDF on a line-by-line basis to avoid loading entire SDF into memory.

    https://en.wikipedia.org/wiki/Chemical_table_file#SDF
    """
    current_molfile = ""
    # TODO: guard file opening.
    with gzip.open(sdf_path, "rb") as gzipped_sdf:
        for gunzipped_line in gzipped_sdf:
            line = gunzipped_line.decode("utf-8", "backslashreplace")
            # TODO: harden SDF parsing according to
            # http://www.dalkescientific.com/writings/diary/archive/2020/09/18/handling_the_sdf_record_delimiter.html
            if "$$$$" in line:
                yield current_molfile
                current_molfile = ""
            else:
                current_molfile += line


def _produce_molfiles(
    molfile_queue: multiprocessing.Queue, sdf_path: str, n_poison_pills: int
):
    for molfile in _read_molfiles_from_zipped_sdf(sdf_path):
        molfile_queue.put(molfile)

    for _ in range(n_poison_pills):
        molfile_queue.put("DONE")  # poison pill: tell consumer processes we're done

    print(f"{utils.get_current_time()}: Done producing molfiles.")


def _consume_molfiles(
    molfile_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    consumer_function: Callable,
    process_id: int,
):
    for molfile in iter(molfile_queue.get, "DONE"):
        result_queue.put(consumer_function(molfile))

    result_queue.put(f"DONE")

    print(f"{utils.get_current_time()}: Process {process_id} done consuming molfiles.")


def run(
    sdf_path: str,
    log_db: sqlite3.Connection,
    consumer_function: Callable,
    number_of_consumer_processes: int,
):
    molfile_queue: multiprocessing.Queue = multiprocessing.Queue()  # TODO: limit size?
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    print(f"{utils.get_current_time()}: Starting producer process.")
    producer_process = multiprocessing.Process(
        target=_produce_molfiles,
        args=(molfile_queue, sdf_path, number_of_consumer_processes),
    )
    producer_process.start()

    print(
        f"{utils.get_current_time()}: Distributing consumer function over {number_of_consumer_processes} processes."
    )
    consumer_processes = [
        multiprocessing.Process(
            target=_consume_molfiles,
            args=(
                molfile_queue,
                result_queue,
                consumer_function,
                process_id,
            ),
        )
        for process_id in range(number_of_consumer_processes)
    ]
    for consumer_process in consumer_processes:
        consumer_process.start()

    number_of_finished_consumer_processes = 0
    print(f"{utils.get_current_time()}: Starting logging results.")
    while number_of_finished_consumer_processes < number_of_consumer_processes:
        result = result_queue.get()  # blocks until result is available
        if result == "DONE":
            number_of_finished_consumer_processes += 1
            continue
        utils.log_result(log_db, result)

    # processes won't join before all queues their interacting with are empty
    producer_process.join()
    for consumer_process in consumer_processes:
        consumer_process.join()
