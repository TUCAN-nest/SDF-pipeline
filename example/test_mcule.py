import re
from typing import Final
from sdf_pipeline import drivers
from consumers import regression_consumer

SDF_PATH: Final = "example/data/mcule_2000.sdf.gz"


def get_mcule_id(molfile: str) -> str:
    molfile_id_pattern = re.compile(r"<Mcule_ID>(.*?)>", re.DOTALL)
    molfile_id_match = molfile_id_pattern.search(molfile)
    molfile_id = molfile_id_match.group(1).strip() if molfile_id_match else ""
    return molfile_id


if __name__ == "__main__":
    args = drivers.parse_cli_args()

    exit_code = 0

    if args.test_type == "regression":
        if args.compute_reference_result:
            exit_code = drivers.regression_reference(
                sdf_path=SDF_PATH,
                log_path=args.result_destination,
                consumer_function=regression_consumer,
                get_molfile_id=get_mcule_id,
            )
        else:
            exit_code = drivers.regression(
                sdf_path=SDF_PATH,
                log_path=args.result_destination,
                reference_path=args.reference_result,
                consumer_function=regression_consumer,
                get_molfile_id=get_mcule_id,
            )

    raise SystemExit(exit_code)
