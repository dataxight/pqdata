from __future__ import annotations

import json
import os
import shutil
from collections import OrderedDict
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from pyarrow import ArrowInvalid
from pyarrow import parquet as pq
from scipy.sparse import coo_matrix, issparse

if TYPE_CHECKING:
    from anndata import AnnData
    from mudata import MuData


def write_table(df, path: str, key: str, colnames=None, compression: str | None = None):
    table = None

    if hasattr(df, "to_parquet"):

        def f(x):
            return partial(x.to_parquet)

        # if pandas.DataFrame, .pandas_metadata will be available in the schema
    else:
        if colnames is None:
            if hasattr(df, "column_names"):
                # PyArrow Table (should be written as per above)
                colnames = df.column_names
            elif hasattr(df, "columns"):
                # Pandas DataFrame (should have .to_parquet)
                colnames = df.columns
            else:
                colnames = [f"{i+1}" for i in range(df.shape[1])]

        if not isinstance(df, pa.Table):
            # vector or structured array?
            if df.ndim == 1:
                if df.dtype.names is not None:
                    colnames = df.dtype.names
                table = pa.Table.from_arrays(
                    [df],
                    names=colnames,
                )
            else:
                try:
                    table = pa.table({colnames[j]: column for j, column in enumerate(df.T)})
                except ArrowInvalid:
                    import numpy as np

                    table = pa.table(
                        {colnames[j]: np.array(column) for j, column in enumerate(df.T)}
                    )
            schema = table.schema.with_metadata(
                {
                    "array": json.dumps(
                        {
                            "shape": df.shape,
                            "class": {
                                "module": df.__class__.__module__,
                                "name": df.__class__.__name__,
                            },
                        }
                    )
                }
            )
            table = table.replace_schema_metadata(metadata=schema.metadata)

        def f(x):
            return partial(pq.write_table, x, compression=compression)

    filepath = Path(path) / f"{key}.parquet"

    if table is None:
        return f(df)(filepath)

    f(table)(filepath)


def write_sparse(mx, path: str, key: str, compression: str | None = None):
    import json

    if issparse(mx):
        if isinstance(mx, coo_matrix):
            x = mx
        else:
            x = mx.tocoo()
    else:
        raise ValueError("Matrix is not sparse")

    filepath = Path(path) / f"{key}.parquet"

    df = pa.table({"row": x.row, "col": x.col, "data": x.data})

    schema = df.schema.with_metadata(
        {
            "array": json.dumps(
                {
                    "shape": x.shape,
                    "class": {
                        "module": mx.__class__.__module__,
                        "name": mx.__class__.__name__,
                    },
                }
            )
        }
    )
    df = df.replace_schema_metadata(metadata=schema.metadata)

    pq.write_table(df, filepath, compression=compression)


def return_or_write(d, parentpath, globalpath, compression: str | None = None):
    # Deprecated in anndata:
    # from anndata.compat._overloaded_dict import OverloadedDict

    # OrderedDict, OverloadedDict -> Dict
    if isinstance(d, dict) or isinstance(d, OrderedDict) or type(d).__name__ == "OverloadedDict":
        new_d = {}
        for k, v in d.items():
            if hasattr(v, "ndim") and v.ndim == 0:  # numpy scalars
                v = v.item()
            maybe_write = return_or_write(
                v, Path(parentpath) / k, globalpath, compression=compression
            )
            if maybe_write is not None:
                new_d[k] = maybe_write
        return new_d
    elif hasattr(d, "ndim") and d.ndim != 0:  # numpy scalars
        rootpath, parentkey = os.path.split(parentpath)
        if d.ndim == 1 and d.dtype.names is None:  # account for structured arrays
            table = pa.Table.from_arrays(
                [d],
                names=[parentkey],
            )

            schema = table.schema.with_metadata(
                {
                    "array": json.dumps(
                        {
                            "shape": d.shape,
                            "class": {
                                "module": d.__class__.__module__,
                                "name": d.__class__.__name__,
                            },
                        }
                    )
                }
            )
            table = table.replace_schema_metadata(metadata=schema.metadata)

        elif d.ndim == 1:  # structured arrays
            import pandas as pd

            table = pd.DataFrame(d)
            table.columns = d.dtype.names
        else:
            table = d
        elem_path = Path(globalpath) / rootpath
        Path(elem_path).mkdir(parents=True, exist_ok=True)
        write_table(table, elem_path, parentkey, compression=compression)
        return None
    else:
        return d


def write_json_and_maybe_tables(struct: dict, path: str, key: str, compression: str | None = None):

    key_simple = return_or_write(struct, "", Path(path) / key, compression=compression)

    key_path = f"{Path(path) / key}.json"

    with Path(key_path).open("w") as key_file:
        json.dump(key_simple, key_file)


def _write_data(
    data: AnnData | MuData,
    path: str,
    *,
    overwrite: bool = False,
    compression: str | None = "snappy",
    **kwargs,
):

    if Path(path).exists() and overwrite:
        shutil.rmtree(path)
    Path(path).mkdir()

    attributes: dict[str, Any] = {}

    # obs / var
    for key in ["obs", "var"]:
        if hasattr(data, key):
            elem = getattr(data, key)
            if elem is not None:
                if len(elem) > 0:
                    # NOTE: Handle TypeError:
                    #       Object of type bool_ is not JSON serializable
                    try:
                        write_table(elem, path, key, compression=compression)
                    except TypeError:
                        for c in elem.columns[elem.dtypes == "category"]:
                            elem[c] = elem[c].astype(str).astype("category")

    # X (AnnData)
    if hasattr(data, "X") and data.X is not None:
        if issparse(data.X):
            write_sparse(data.X, path, "X", compression=compression)
        else:
            write_table(
                data.X,
                path,
                "X",
                colnames=data.var_names.values,
                compression=compression,
            )

    # raw (AnnData)
    # NOTE: this is only to support legacy objects and files
    # NOTE: new objects should not use .raw
    if hasattr(data, "raw") and data.raw is not None:
        rawpath = Path(path) / "raw"
        Path(rawpath).mkdir(parents=True)
        # raw.X
        if issparse(data.raw.X):
            write_sparse(data.raw.X, rawpath, "X", compression=compression)
        else:
            write_table(
                data.raw.X,
                rawpath,
                "X",
                colnames=data.raw.var_names.values,
                compression=compression,
            )

        rawvarnames = data.raw.var_names.values

        # raw.var
        write_table(data.raw.var, rawpath, "var", colnames=rawvarnames, compression=compression)

        # raw.obs
        rawobsnames = data.raw.obs_names.values
        from pandas import DataFrame

        rawobs = DataFrame(index=rawobsnames)
        write_table(rawobs, rawpath, "obs", compression=compression)

        # raw.varm
        if data.raw.varm is not None and len(data.raw.varm) > 0:
            rawvarmpath = Path(rawpath) / "varm"
            Path(rawvarmpath).mkdir(parents=True)
            for item in data.raw.varm:
                # TODO: account for 1d arrays
                write_table(
                    data.raw.varm[item],
                    rawvarmpath,
                    item,
                    colnames=data.raw.var.columns.values,
                    compression=compression,
                )

    # obsm / varm / obsp / varp / layers / obsmap / varmap
    for key in ["obsm", "varm", "obsp", "varp", "layers", "obsmap", "varmap"]:
        if hasattr(data, key):
            elem = getattr(data, key)
            if elem is not None and len(elem) > 0:
                subpath = Path(path) / key
                Path(subpath).mkdir(parents=True)
                for item in elem.keys():
                    # Do not serialise 1D arrays from earlier MuData files
                    if key in ["obsm", "varm"] and hasattr(data, "mod") and item in data.mod:
                        continue
                    elem_item = elem[item]
                    colnames = None
                    # TODO: use metadata for that
                    if key == "layers" or key == "varp":
                        # For layers, write var_names as columns
                        colnames = data.var_names.values
                    elif key == "obsp":
                        colnames = data.obs_names.values
                    elif key == "obsmap" or key == "varmap":
                        from pandas import DataFrame

                        colnames = [item]

                    if issparse(elem_item):
                        write_sparse(
                            elem_item,
                            subpath,
                            item,
                            compression=compression,
                        )
                    else:
                        write_table(
                            elem_item,
                            subpath,
                            item,
                            colnames=colnames,
                            compression=compression,
                        )

    # mod (MuData)
    if hasattr(data, "mod") and data.mod is not None:
        mod_subpath = Path(path) / "mod"
        if not Path(mod_subpath).exists():
            Path(mod_subpath).mkdir()
        for mod_key, modality in data.mod.items():
            mod_path = Path(mod_subpath) / mod_key
            _write_data(
                modality,
                mod_path,
                overwrite=overwrite,
                compression=compression,
                **kwargs,
            )

        attributes["mod"] = {
            "order": list(data.mod.keys()),
        }
        if hasattr(data, "_axis") and data._axis is not None:
            attributes["mod"]["axis"] = data._axis

    # uns
    if hasattr(data, "uns") and data.uns is not None:
        if len(data.uns) > 0:
            # uns_path = Path(path) / "uns.json"
            write_json_and_maybe_tables(data.uns, path, "uns", compression=compression)

    # serialisation metadata
    if len(attributes) > 0:
        # write to pqdata.json
        write_json_and_maybe_tables(attributes, path, "pqdata", compression=compression)


write_anndata = _write_data
write_mudata = _write_data
