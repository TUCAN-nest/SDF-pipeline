import re
from typing import Final
from sdf_pipeline import drivers
from .consumers import regression_consumer

PWD: Final = "example/data/mcule"


def get_mcule_id(molfile: str) -> str:
    molfile_id_pattern = re.compile(r"<Mcule_ID>(.*?)>", re.DOTALL)
    molfile_id_match = molfile_id_pattern.search(molfile)
    molfile_id = molfile_id_match.group(1).strip() if molfile_id_match else ""
    return molfile_id


if __name__ == "__main__":
    exit_code = 0

    exit_code = drivers.regression_reference(
        sdf_path=PWD + "/mcule_2000.sdf.gz",
        log_path=PWD + "/mcule_2000_regression_reference.sqlite",
        consumer_function=regression_consumer,
        get_molfile_id=get_mcule_id,
    )
    exit_code = drivers.regression(
        sdf_path=PWD + "/mcule_2000.sdf.gz",
        log_path=PWD + "/mcule_2000_regression.sqlite",
        reference_path=PWD + "/mcule_2000_regression_reference.sqlite",
        consumer_function=regression_consumer,
        get_molfile_id=get_mcule_id,
    )

    raise SystemExit(exit_code)
