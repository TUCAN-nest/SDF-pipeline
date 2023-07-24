"""Download gzipped SDF files from ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF/.

https://openbook.rheinwerk-verlag.de/python/34_003.html
https://docs.python.org/3/library/ftplib.html
https://pubchem.ncbi.nlm.nih.gov/docs/rdf-ftp
"""

from ftplib import FTP
from ftplib import all_errors as FTPException
import hashlib
from pathlib import Path
from typing import Iterator


class LineData(list):
    def __call__(self, line: str):
        super().append(line)


class MD5:
    """Compute MD5 hash from byte stream."""

    def __init__(self):
        self.hash_function = hashlib.md5()

    def __call__(self, block: bytes):
        self.hash_function.update(block)

    @property
    def hash(self) -> str:
        return self.hash_function.hexdigest()


def _fetch_gzipped_sdf_filenames(ftp_client: FTP) -> list[str]:
    """Fetch names of all gzipped SDF files from FTP server."""
    filenames = LineData()
    ftp_client.retrlines("LIST", filenames)

    return [
        filename.split()[-1].strip()
        for filename in filenames
        if filename.endswith(".sdf.gz")
    ]


def _fetch_gzipped_sdf(
    filename: str, destination_directory: str, ftp_client: FTP
) -> str:
    """Fetch gzipped SDF from FTP server.

    Validates the gzipped SDF and writes it to the file system.
    Streams data rather than downloading entire SDF file.
    Streaming is important due to large file sizes.
    """
    md5 = MD5()

    def distribute_ftp_callback(block: bytes):
        md5(block)
        gzipped_sdf.write(block)

    filepath = Path(destination_directory).joinpath(filename)
    with filepath.open("wb") as gzipped_sdf:
        try:
            ftp_client.retrbinary(f"RETR {filename}", distribute_ftp_callback)
        except FTPException as exception:
            print(exception)
            return ""

    if md5.hash != _fetch_gzipped_sdf_hash(filename, ftp_client):
        print(
            f"The hash of {filepath.as_posix()} doesn't match it's hash on the FTP server. Removing the file locally."
        )
        filepath.unlink()
        return ""

    return filepath.as_posix()


def _fetch_gzipped_sdf_hash(filename: str, ftp_client: FTP) -> str:
    """Fetch MD5 hash from FTP server."""
    md5 = LineData()
    try:
        ftp_client.retrlines(f"RETR {filename}.md5", md5)
    except FTPException as exception:
        print(exception)
        return ""

    return md5[0].split()[0].strip()


def download_all_sdf(destination_directory: str) -> Iterator[str]:
    """Generator yielding file paths of successfully downloaded SDF."""
    with FTP("ftp.ncbi.nlm.nih.gov") as ftp_client:
        ftp_client.login()
        ftp_client.cwd("pubchem/Compound/CURRENT-Full/SDF/")

        # filename = "Compound_033500001_034000000.sdf.gz"  # Compound_033500001_034000000.sdf.gz good for prototyping since it's only 20M large
        for filename in _fetch_gzipped_sdf_filenames(ftp_client):
            if filepath := _fetch_gzipped_sdf(
                filename, destination_directory, ftp_client
            ):
                yield filepath


def get_id(molfile: str) -> str:
    return molfile.split()[0].strip()
