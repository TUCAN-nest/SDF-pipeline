from typing import Callable
from sdf_pipeline import utils


def regression_consumer(molfile: str, get_molfile_id: Callable) -> utils.ConsumerResult:
    return utils.ConsumerResult(
        "regression",
        utils.get_current_time(),
        get_molfile_id(molfile),
        str(len(molfile)),
    )
