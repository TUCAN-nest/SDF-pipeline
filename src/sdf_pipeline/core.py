import multiprocessing
import gzip
from typing import Callable, TYPE_CHECKING
from collections.abc import Generator

if TYPE_CHECKING:
    # https://adamj.eu/tech/2021/05/13/python-type-hints-how-to-fix-circular-imports/
    from sdf_pipeline.drivers import ConsumerResult


def read_records_from_gzipped_sdf(sdf_path: str) -> Generator[str, None, None]:
    # https://en.wikipedia.org/wiki/Chemical_table_file#SDF"
    current_record = ""
    # TODO: guard file opening.
    with gzip.open(sdf_path, "rb") as gzipped_sdf:
        # decompress SDF line-by-line to avoid loading entire SDF into memory
        for decompressed_line in gzipped_sdf:
            decoded_line = decompressed_line.decode("utf-8", "backslashreplace")
            current_record += decoded_line
            if decoded_line.strip() == "$$$$":
                # TODO: harden SDF parsing according to
                # http://www.dalkescientific.com/writings/diary/archive/2020/09/18/handling_the_sdf_record_delimiter.html
                yield current_record
                current_record = ""


def _produce_molfiles(
    molfile_queue: multiprocessing.Queue, sdf_path: str, n_poison_pills: int
) -> None:
    for molfile in read_records_from_gzipped_sdf(sdf_path):
        molfile_queue.put(molfile)

    for _ in range(n_poison_pills):
        molfile_queue.put("DONE")  # poison pill: tell consumer processes we're done


def _consume_molfiles(
    molfile_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    consumer_function: Callable,
) -> None:
    for molfile in iter(molfile_queue.get, "DONE"):
        result_queue.put(consumer_function(molfile))

    result_queue.put(f"DONE")


def run(
    sdf_path: str,
    consumer_function: Callable,
    number_of_consumer_processes: int,
) -> Generator["ConsumerResult", None, None]:
    molfile_queue: multiprocessing.Queue = multiprocessing.Queue()  # TODO: limit size?
    result_queue: multiprocessing.Queue = multiprocessing.Queue()

    producer_process = multiprocessing.Process(
        target=_produce_molfiles,
        args=(molfile_queue, sdf_path, number_of_consumer_processes),
    )
    producer_process.start()

    consumer_processes = [
        multiprocessing.Process(
            target=_consume_molfiles,
            args=(
                molfile_queue,
                result_queue,
                consumer_function,
            ),
        )
        for process_id in range(number_of_consumer_processes)
    ]
    for consumer_process in consumer_processes:
        consumer_process.start()

    number_of_finished_consumer_processes = 0
    while number_of_finished_consumer_processes < number_of_consumer_processes:
        result = result_queue.get()  # blocks until result is available
        if result == "DONE":
            number_of_finished_consumer_processes += 1
            continue
        yield result

    # processes won't join before all queues their interacting with are empty
    producer_process.join()
    for consumer_process in consumer_processes:
        consumer_process.join()
