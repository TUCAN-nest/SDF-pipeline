"""Download gzipped SDF files from ftp.ncbi.nlm.nih.gov/pubchem/.

https://openbook.rheinwerk-verlag.de/python/34_003.html
https://docs.python.org/3/library/ftplib.html
https://pubchem.ncbi.nlm.nih.gov/docs/downloads#section=From-the-PubChem-FTP-Site
"""

from ftplib import FTP
from ftplib import all_errors as FTPException
import hashlib
from pathlib import Path
from typing import Iterator
from contextlib import contextmanager


@contextmanager
def pubchem_ftp_client(dataset_directory: str):
    client = FTP("ftp.ncbi.nlm.nih.gov")
    client.login()
    client.cwd(dataset_directory)
    try:
        yield client
    except FTPException as exception:
        print(exception)
    finally:
        client.close()


class LineData:
    def __init__(self):
        self.content = ""

    def __call__(self, line):
        self.content += line.split()[0].strip()


class MD5:
    """Compute MD5 hash from byte stream."""

    def __init__(self):
        self.hash_function = hashlib.md5()

    def __call__(self, block: bytes):
        self.hash_function.update(block)

    @property
    def hash(self) -> str:
        return self.hash_function.hexdigest()


def _fetch_gzipped_sdf_filenames(dataset_directory: str) -> list[str]:
    """Fetch names of all gzipped SDF from FTP server."""

    with pubchem_ftp_client(dataset_directory) as client:
        return [
            file_description[0]
            for file_description in list(client.mlsd())
            if file_description[0].endswith(".sdf.gz")
        ]


def _fetch_gzipped_sdf(
    filename: str,
    destination_directory: str,
    dataset_directory: str,
    overwrite_file: bool,
) -> str:
    """Fetch gzipped SDF from FTP server.

    Validates the gzipped SDF and writes it to the file system.
    """
    filepath = Path(destination_directory).joinpath(filename)
    if filepath.exists() and not overwrite_file:
        print(f"{filepath.as_posix()} already exists. Skipping download.")
        return ""

    md5 = MD5()

    def distribute_ftp_callback(block: bytes):
        md5(block)
        gzipped_sdf.write(block)

    with filepath.open("wb") as gzipped_sdf, pubchem_ftp_client(
        dataset_directory
    ) as client:
        client.retrbinary(f"RETR {filename}", distribute_ftp_callback)

    md5_hash_from_ftp_server = _fetch_gzipped_sdf_hash(filename, dataset_directory)
    if md5_hash_from_ftp_server:
        # Some PubChem datasets (e.g., Compound 3D) don't have MD5 hashes.
        if md5.hash != _fetch_gzipped_sdf_hash(filename, dataset_directory):
            print(
                f"The hash of {filepath.as_posix()} doesn't match it's corresponding hash on the FTP server. Removing the file locally."
            )
            filepath.unlink()
            return ""

    return filepath.as_posix()


def _fetch_gzipped_sdf_hash(filename: str, dataset_directory: str) -> str:
    """Fetch MD5 hash from FTP server."""
    md5 = LineData()

    with pubchem_ftp_client(dataset_directory) as client:
        try:
            client.retrlines(f"RETR {filename}.md5", md5)
        except FTPException:
            # Some PubChem datasets (e.g., Compound 3D) don't have MD5 hashes.
            pass

    return md5.content


def download_all_sdf(
    destination_directory: str,
    dataset_directory: str,
    overwrite_files: bool = False,
) -> Iterator[str]:
    """Generator yielding file paths of successfully downloaded gzipped SDF.

    `dataset_directory` can be one of
    `pubchem/Compound/CURRENT-Full/SDF/`,
    `pubchem/Substance/CURRENT-Full/SDF/`, or
    `pubchem/Compound_3D/01_conf_per_cmpd/SDF`.
    """
    for filename in _fetch_gzipped_sdf_filenames(dataset_directory):
        if filepath := _fetch_gzipped_sdf(
            filename, destination_directory, dataset_directory, overwrite_files
        ):
            yield filepath


def get_id(molfile: str) -> str:
    return molfile.split()[0].strip()
