import pytest
from sdf_pipeline import pubchem


@pytest.mark.parametrize(
    "dataset_directory, expected_sdf_paths",
    [
        (
            "pubchem/Compound/CURRENT-Full/SDF/",
            [
                "Compound_000000001_000500000.sdf.gz",
                "Compound_001000001_001500000.sdf.gz",
            ],
        ),
        (
            "pubchem/Substance/CURRENT-Full/SDF/",
            [
                "Substance_000500001_001000000.sdf.gz",
                "Substance_000000001_000500000.sdf.gz",
            ],
        ),
        (
            "pubchem/Compound_3D/01_conf_per_cmpd/SDF",
            [
                "00000001_00025000.sdf.gz",
                "00025001_00050000.sdf.gz",
            ],
        ),
    ],
)
def test_download_all_sdf(tmp_path, dataset_directory, expected_sdf_paths):
    sdf_path_generator = pubchem.download_all_sdf(
        destination_directory=str(tmp_path), dataset_directory=dataset_directory
    )
    sdf_paths = {next(sdf_path_generator) for _ in range(2)}

    assert sdf_paths == {str(tmp_path / sdf_path) for sdf_path in expected_sdf_paths}
    assert all((tmp_path / sdf_path).exists() for sdf_path in sdf_paths)


@pytest.mark.parametrize(
    "dataset_directory, expected_n_sdf_paths",
    [
        ("pubchem/Compound/CURRENT-Full/SDF/", 338),
        ("pubchem/Substance/CURRENT-Full/SDF/", 894),
        ("pubchem/Compound_3D/01_conf_per_cmpd/SDF", 6646),
    ],
)
def test_fetch_gzipped_sdf_filenames(dataset_directory, expected_n_sdf_paths):
    sdf_paths = pubchem._fetch_gzipped_sdf_filenames(dataset_directory)

    assert len(sdf_paths) == expected_n_sdf_paths


@pytest.mark.parametrize(
    "dataset_directory, filename, expected_hash",
    [
        (
            "pubchem/Compound/CURRENT-Full/SDF/",
            "Compound_000000001_000500000.sdf.gz",
            "81d318fd569898ffc1506478d6f3389b",
        ),
        (
            "pubchem/Substance/CURRENT-Full/SDF/",
            "Substance_000500001_001000000.sdf.gz",
            "5365255fe6acbe94a9d48c7ca1a745b9",
        ),
        ("pubchem/Compound_3D/01_conf_per_cmpd/SDF", "00000001_00025000.sdf.gz", ""),
    ],
)
def test_fetch_gzipped_sdf_hash(dataset_directory, filename, expected_hash):
    assert pubchem._fetch_gzipped_sdf_hash(filename, dataset_directory) == expected_hash
