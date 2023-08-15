from sdf_pipeline import pubchem
from sdf_pipeline.utils import get_current_time
from pathlib import Path


if __name__ == "__main__":
    pubchem_sdfs = pubchem.download_all_sdf(
        destination_directory=Path(__file__).parent.absolute()
    )
    for _ in range(2):
        print(f"{get_current_time()}: Downloaded {next(pubchem_sdfs)}")
