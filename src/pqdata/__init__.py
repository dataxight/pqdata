"""pqdata: Parquet-based I/O for scverse data structures."""

try:  # See https://github.com/maresb/hatch-vcs-footgun-example
    from setuptools_scm import get_version

    __version__ = get_version(root="../..", relative_to=__file__)
except (ImportError, LookupError):
    try:
        from ._version import __version__
    except ModuleNotFoundError:
        raise RuntimeError("pqdata is not correctly installed. Please install it, e.g. with pip.")

from .core import open_storage as open
from .io.read import read_anndata, read_mudata
from .io.write import write_anndata, write_mudata

__all__ = ["open", "read_anndata", "read_mudata", "write_anndata", "write_mudata", "__version__"]

