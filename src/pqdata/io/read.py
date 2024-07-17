import json
import os
from os import PathLike
from pathlib import Path
from typing import Any, Literal
from warnings import warn

from pyarrow import parquet as pq
from scipy.sparse import coo_matrix


def read_table(path: str, kind: Literal["array", "dataframe", "polars"] = None):

    table = pq.read_table(path)
    table_meta = table.schema.metadata

    if kind is None:
        if table_meta is None:
            kind = "dataframe"
        elif b"array" in table_meta:
            kind = "array"
        elif b"pandas" in table_meta:
            kind = "dataframe"

    if kind == "dataframe":
        # TODO: dataframe backends (Polars)
        return table.to_pandas()
    elif kind == "array":
        # TODO: array backends (JAX)
        is_coo = all([c in table.column_names for c in ["data", "row", "col"]])
        is_coo = all([c in ["data", "row", "col"] for c in table.column_names]) and is_coo
        if is_coo:
            shape = None
            matrix_func = None

            if table.schema.metadata is not None and b"array" in table.schema.metadata:
                metadata = json.loads(table.schema.metadata[b"array"])
                if "shape" in metadata:
                    shape = metadata["shape"]
                if "class" in metadata:
                    module, name = (
                        metadata["class"]["module"],
                        metadata["class"]["name"],
                    )
                    try:
                        matrix_func = getattr(__import__(module, fromlist=[name]), name)
                    except Exception as e:
                        warn(str(e))

            mx = coo_matrix((table["data"], (table["row"], table["col"])), shape=shape)

            if matrix_func is not None:
                try:
                    mx = matrix_func(mx)
                except Exception as e:
                    warn(str(e))

            return mx

        else:
            x = table.to_pandas().to_numpy()
            if table_meta is not None:
                shape = json.loads(table_meta.get(b"array", b"{}")).get("shape", None)
                if shape is not None:
                    if len(shape) == 1:
                        x = x.squeeze()
                    if x.shape != tuple(shape):
                        warn(
                            "Shapes for some array might not have been "
                            "properly recorded and recovered"
                        )
            return x
    else:
        return table


def read_sparse(path: str):
    table = pq.read_table(path)
    is_coo = all([c in table.column_names for c in ["data", "row", "col"]])
    if not is_coo:
        raise NotImplementedError

    return coo_matrix((table["data"], (table["row"], table["col"])))


def put_into_dict(d: dict, key: str | PathLike, v: Any):
    key = str(key)  # PosixPath -> str
    key_levels = os.path.normpath(key).split(os.path.sep)
    dict_loc = d
    for level in key_levels[:-1]:
        dict_loc[level] = dict_loc.get(level, dict())
        dict_loc = dict_loc[level]

    dict_loc[key_levels[-1]] = v

    return


def read_tables_add_to_dict(path: PathLike, d: dict):
    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = Path(root) / file
            table_loc = file_path.relative_to(path).with_suffix("")
            table = read_table(file_path)
            put_into_dict(d, table_loc, table)

    return


def _read_data(path: str):

    data_dict: dict[str, Any] = {}

    # serialisation metadata
    attributes: dict[str, Any] = {}
    attrs_json_path = Path(path) / "pqdata.json"
    if Path(attrs_json_path).exists():
        with Path(attrs_json_path).open() as file:
            attributes = json.load(file)

    # obs / var
    for key in ["obs", "var"]:
        elem_path = Path(path) / f"{key}.parquet"
        if Path(elem_path).exists():
            data_dict[key] = read_table(elem_path, kind="dataframe")

    # X (AnnData)
    x_path = Path(path) / "X.parquet"
    if Path(x_path).exists():
        data_dict["X"] = read_table(x_path, kind="array")

    # raw (AnnData)
    raw_path = Path(path) / "raw"
    if Path(raw_path).exists():
        raw_dict = {}
        read_tables_add_to_dict(raw_path, raw_dict)
        from anndata import AnnData

        raw = AnnData(**raw_dict)
        data_dict["raw"] = raw

    # obsm / varm / obsp / varp / layers / obsmap / varmap
    for key in ["obsm", "varm", "obsp", "varp", "layers", "obsmap", "varmap"]:
        elem_path = Path(path) / key
        if Path(elem_path).exists():
            data_dict[key] = {}
            for file in os.listdir(elem_path):
                item_name = str(Path(file).with_suffix(""))
                item_path = Path(elem_path) / file
                data_dict[key][item_name] = read_table(item_path)
    # uns
    uns_json_path = Path(path) / "uns.json"
    if Path(uns_json_path).exists():
        with Path(uns_json_path).open() as file:
            data_dict["uns"] = json.load(file)
    else:
        data_dict["uns"] = {}

    uns_dir_path = Path(path) / "uns"
    if Path(uns_dir_path).exists():
        read_tables_add_to_dict(uns_dir_path, data_dict["uns"])

    # mod (MuData)
    mod_path = Path(path) / "mod"
    if Path(mod_path).exists():
        data_dict["mod"] = dict()
        modalities = os.listdir(mod_path)
        for m in modalities:
            mpath = Path(mod_path) / m
            # TODO: Allow for nested MuData
            data_dict["mod"][m] = read_anndata(mpath)

        mod_dict = attributes.get("mod")
        if "order" in mod_dict:
            mod_order = mod_dict["order"]
            if all([m in mod_order for m in modalities]):
                data_dict["mod"] = {m: data_dict["mod"][m] for m in mod_order if m in modalities}

        if "axis" in mod_dict:
            data_dict["axis"] = mod_dict["axis"]

    return data_dict


def read_anndata(path: str):
    from anndata import AnnData

    data_dict = _read_data(path)
    return AnnData(**data_dict)


def read_mudata(path: str):
    from mudata import MuData

    data_dict = _read_data(path)
    return MuData._init_from_dict_(**data_dict)
