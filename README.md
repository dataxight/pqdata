[![PyPi version](https://img.shields.io/pypi/v/pqdata)](https://pypi.org/project/pqdata)

# pqdata

Experimental Parquet-based I/O for [scverse](https://scverse.org) data structures.

> [!WARNING]
> This package is experimental, and API can change between versions as well as the file structure.

## Installation

```
pip install pqdata
# or
pip install git+https://github.com/gtca/pqdata
```

## Motivation

`scverse` data formats such as [AnnData](https://github.com/scverse/anndata) and [MuData](https://github.com/scverse/mudata) are serialized to HDF5 (`.h5ad`, `.h5mu`) or [Zarr](https://zarr.dev/) files by default. To query individual parts of the HDF5 files, lower-level access interfaces such as `h5py` or smart caching such as [`shadows`](https://github.com/scverse/shadows) are required, and they inevitably load the necessary tables in memory. Zarr files are in fact directories allowing to easily access different parts of the storage however the data is stored in binary blobs, which severely limits the range of tools that can work on these blobs.

With `pqdata`, all the components of the data storage are represented in `.parquet` or `.json` files, with the directory structure recapitulating the nested hierarchy of object's attributes.
[Parquet files](https://parquet.apache.org/) are efficient for column-oriented data storage and retrieval. As the industry standard, they enable analytical applications such as [DuckDB](https://duckdb.org/docs/data/parquet/overview.html) or [ClickHouse](https://clickhouse.com/docs/en/integrations/data-formats/parquet) to be readily applied on the tabular data.

## Features and integrations

- [x] AnnData and MuData I/O
- [x] Low-level storage access akin to `h5py` and `zarr` libraries

### I/O

[Example notebook](/docs/examples/pqdata-serialization-intro.ipynb) | [AnnData/MuData I/O](/docs/examples/getting-started-anndata-mudata-parquet-serialization.ipynb)

I/O with `pqdata` works like this:

```py
from pqdata import write_anndata, write_mudata
write_anndata(adata, "pbmc3k_anndata.pqdata")
write_mudata(mdata, "pbmc5k_citeseq_mudata.pqdata")

from pqdata import read_anndata, read_mudata
adata = read_anndata("pbmc3k_anndata.pqdata")
mdata = read_mudata("pbmc5k_citeseq_mudata.pqdata")
```
