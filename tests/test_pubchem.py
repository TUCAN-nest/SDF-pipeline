from sdf_pipeline import pubchem


def test_download_all_sdf(tmp_path):
    sdf_path_generator = pubchem.download_all_sdf(destination_directory=str(tmp_path))
    sdf_paths = {next(sdf_path_generator) for _ in range(2)}

    assert sdf_paths == {
        str(tmp_path / sdf_path)
        for sdf_path in [
            "Compound_000000001_000500000.sdf.gz",
            "Compound_001000001_001500000.sdf.gz",
        ]
    }
    assert all((tmp_path / sdf_path).exists() for sdf_path in sdf_paths)
