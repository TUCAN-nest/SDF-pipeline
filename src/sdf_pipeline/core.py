import gzip
from concurrent.futures import ProcessPoolExecutor, as_completed, process
from typing import Callable, TYPE_CHECKING
from collections.abc import Generator
from sdf_pipeline import logger

if TYPE_CHECKING:
    # https://adamj.eu/tech/2021/05/13/python-type-hints-how-to-fix-circular-imports/
    from sdf_pipeline.drivers import ConsumerResult


def read_records_from_gzipped_sdf(sdf_path: str) -> Generator[str, None, None]:
    # https://en.wikipedia.org/wiki/Chemical_table_file#SDF"
    current_record = ""
    # TODO: guard file opening.
    with gzip.open(sdf_path, "rb") as gzipped_sdf:
        # Decompress SDF line-by-line to avoid loading entire SDF into memory.
        for decompressed_line in gzipped_sdf:
            decoded_line = decompressed_line.decode("utf-8", "backslashreplace")
            current_record += decoded_line
            if decoded_line.strip() == "$$$$":
                # TODO: harden SDF parsing according to
                # http://www.dalkescientific.com/writings/diary/archive/2020/09/18/handling_the_sdf_record_delimiter.html.
                yield current_record
                current_record = ""

    return None


def run(
    sdf_path: str, consumer_function: Callable, number_of_consumer_processes: int
) -> Generator["ConsumerResult", None, None]:

    try:
        with ProcessPoolExecutor(
            max_workers=number_of_consumer_processes
        ) as process_pool:
            futures = (
                process_pool.submit(consumer_function, molfile)
                for molfile in read_records_from_gzipped_sdf(sdf_path)
            )
            """
            `as_completed` isn't iterating `futures` lazily. It instantiates a set of futures:
            https://github.com/python/cpython/blob/f383ca1a6fa1a2a83c8c1a0e56cf997c77fa2893/Lib/concurrent/futures/_base.py#L220.
            In case of memory constraints, consider chunking `futures`: https://bugs.python.org/issue34168.
            """
            for future in as_completed(futures):
                exception = future.exception()
                if exception is not None:
                    logger.error(f"could not process {sdf_path}: {exception}")

                    raise exception  # Dead programs don't tell lies.

                yield future.result()

    except process.BrokenProcessPool as exception:
        """
        `exception` can be thrown in two ways with (slightly) different messages:
        1) While processing a future in the `as_completed` loop above,
            in which case `exception` is re-raised below.
            See https://github.com/python/cpython/blob/7f074a771bc4e3e299799fabf9b054a03f6693d2/Lib/concurrent/futures/process.py#L489
            and https://discuss.python.org/t/as-completed-not-yielding-futures-that-raised-until-pool-exits/39920.
        2) While submitting a task to the `process_pool`.
            See https://github.com/python/cpython/blob/7f074a771bc4e3e299799fabf9b054a03f6693d2/Lib/concurrent/futures/process.py#L793.
        We need this `except` block in order to catch 2). For 1), the inner `raise` would be sufficient.
        """
        logger.error(f"could not process {sdf_path}: {exception}")

        raise exception

    return None
