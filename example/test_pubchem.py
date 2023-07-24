from pathlib import Path
from sdf_pipeline import drivers
from sdf_pipeline import pubchem
from consumers import regression_consumer


if __name__ == "__main__":
    args = drivers.parse_cli_args()

    exit_code = 0

    for sdf_path in (
        Path(__file__)
        .parent.absolute()
        .joinpath("data/pubchem")
        .glob("Compound_*_*.sdf.gz")
    ):
        if args.test_type == "regression":
            if args.compute_reference_result:
                exit_code = max(
                    exit_code,
                    drivers.regression_reference(
                        sdf_path=sdf_path,
                        log_path=f"{sdf_path}_regression_reference.sqlite",
                        consumer_function=regression_consumer,
                        get_molfile_id=pubchem.get_id,
                    ),
                )
            else:
                exit_code = max(
                    exit_code,
                    drivers.regression(
                        sdf_path=sdf_path,
                        log_path=f"{sdf_path}_regression.sqlite",
                        reference_path=f"{sdf_path}_regression_reference.sqlite",
                        consumer_function=regression_consumer,
                        get_molfile_id=pubchem.get_id,
                    ),
                )
