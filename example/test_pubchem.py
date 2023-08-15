from pathlib import Path
from sdf_pipeline import drivers
from sdf_pipeline import pubchem
from .consumers import regression_consumer


if __name__ == "__main__":
    exit_code = 0

    for sdf_path in (
        Path(__file__)
        .parent.absolute()
        .joinpath("data/pubchem")
        .glob("Compound_*_*.sdf.gz")
    ):
        exit_code = max(
            exit_code,
            drivers.regression_reference(
                sdf_path=sdf_path,
                log_path=f"{sdf_path}_regression_reference.sqlite",
                consumer_function=regression_consumer,
                get_molfile_id=pubchem.get_id,
            ),
        )
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

    raise SystemExit(exit_code)
