import os
from typing import Literal, Dict, Any
from warnings import warn

from scipy.sparse import coo_matrix

from pyarrow import parquet as pq
import json


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


def put_into_dict(d: Dict, key: str, v: Any):
    key_levels = os.path.normpath(key).split(os.path.sep)
    dict_loc = d
    for level in key_levels[:-1]:
        dict_loc[level] = dict_loc.get(level, dict())
        dict_loc = dict_loc[level]

    dict_loc[key_levels[-1]] = v

    return


def read_tables_add_to_dict(path: str, d: Dict):

    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            table_loc = os.path.splitext(
                os.path.join(file_path[len(path) :].strip(os.path.sep))
            )[0]

            table = read_table(file_path)

            # table_loc = str.removeprefix(file_path, path)
            put_into_dict(d, table_loc, table)

    return


def _read_data(path: str):

    data_dict: dict[str, Any] = {}

    # serialisation metadata
    attributes: dict[str, Any] = {}
    attrs_json_path = os.path.join(path, "pqdata.json")
    if os.path.exists(attrs_json_path):
        with open(attrs_json_path, "r") as file:
            attributes = json.load(file)

    # obs / var
    for key in ["obs", "var"]:
        elem_path = os.path.join(path, f"{key}.parquet")
        if os.path.exists(elem_path):
            data_dict[key] = read_table(elem_path, kind="dataframe")

    # X (AnnData)
    x_path = os.path.join(path, "X.parquet")
    if os.path.exists(x_path):
        data_dict["X"] = read_table(x_path, kind="array")

    # raw (AnnData)
    raw_path = os.path.join(path, "raw")
    if os.path.exists(raw_path):
        raw_dict = {}
        read_tables_add_to_dict(raw_path, raw_dict)
        from anndata import AnnData

        raw = AnnData(**raw_dict)
        data_dict["raw"] = raw

    # obsm / varm / obsp / varp / layers
    for key in ["obsm", "varm", "obsp", "varp", "layers", "obsmap", "varmap"]:
        elem_path = os.path.join(path, key)
        if os.path.exists(elem_path):
            data_dict[key] = {}
            for file in os.listdir(elem_path):
                item_name = os.path.splitext(file)[0]
                item_path = os.path.join(elem_path, file)
                # TODO: use metadata for that
                data_dict[key][item_name] = read_table(item_path, kind="array")
    # uns
    uns_json_path = os.path.join(path, "uns.json")
    if os.path.exists(uns_json_path):
        with open(uns_json_path, "r") as file:
            data_dict["uns"] = json.load(file)
    else:
        data_dict["uns"] = {}

    uns_dir_path = os.path.join(path, "uns")
    if os.path.exists(uns_dir_path):
        read_tables_add_to_dict(uns_dir_path, data_dict["uns"])

    # mod (MuData)
    mod_path = os.path.join(path, "mod")
    if os.path.exists(mod_path):
        data_dict["mod"] = dict()
        modalities = os.listdir(mod_path)
        for m in modalities:
            mpath = os.path.join(mod_path, m)
            # TODO: Allow for nested MuData
            data_dict["mod"][m] = read_anndata(mpath)

        mod_dict = attributes.get("mod")
        if "order" in mod_dict:
            mod_order = mod_dict["order"]
            if all([m in mod_order for m in modalities]):
                data_dict["mod"] = {
                    m: data_dict["mod"][m] for m in mod_order if m in modalities
                }

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
