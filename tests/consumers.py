from typing import Callable
from sdf_pipeline import utils


def regression_consumer(molfile: str, get_molfile_id: Callable) -> utils.ConsumerResult:
    return utils.ConsumerResult(
        get_molfile_id(molfile),
        utils.get_current_time(),
        "regression",
        str(len(molfile)),
    )
