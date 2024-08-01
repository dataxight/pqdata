import json
import os
from os import PathLike
from pathlib import Path
from typing import Any

import fsspec
from fsspec.mapping import FSMap
from fsspec.spec import AbstractFileSystem
from pyarrow import parquet as pq

from .io.read import read_table

SUPPORTED_EXTENSIONS = ".parquet", ".pq", ".json", ".yaml", ".yml", ".toml"


class Group:
    """
    HDF5/Zarr-like interface to access directory contents
    stored as parquet and json files.
    """

    def __init__(self, path: PathLike | FSMap, origin: PathLike | None = None, fs: AbstractFileSystem | None = None):
        if fs is not None:
            self.fs = fs
            self.path = Path(path)
        else:
            if isinstance(path, FSMap):
                self.fs = path.fs
                self.path = Path(path.root)
            else:
                self.fs = fsspec.filesystem("file")
                self.path = Path(path)
    
        if not self.fs.exists(self.path):
            raise FileNotFoundError(f"Path {self.path} does not exist.")

        if origin is None:
            self.og = self.path
        else:
            self.og = Path(origin)

        self.name = "/"

        # Note there is nothing else happening at this point,
        # no locks are acquired, no files are opened, etc.

    def __repr__(self):
        return f"ParquetStorage({self.path})"

    def __str__(self):
        return f"ParquetStorage at {self.path}"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def __getitem__(self, key: str):
        pass

    # FIXME
    @property
    def attrs(self):
        if self.name.endswith("mod"):
            with self.fs.open(self.path.parent / "pqdata.json") as file:
                attrs = json.load(file)
                try:
                    attrs["mod"]["mod-order"] = attrs["mod"]["order"]
                except KeyError:
                    pass
                return attrs
        else:
            return {}


class GroupAccessor(Group):
    """
    Accessor for directory contents
    """

    def __init__(
        self,
        path: PathLike,
        origin: PathLike | None = None,
        key: str = "",
        memo: dict[str, Any] = None,
        fs: AbstractFileSystem | None = None,
    ):
        super().__init__(path, origin, fs)
        self.name = str(Path(self.name) / key)
        self.contents = memo

    def __getitem__(self, key: str):
        if key == "/":
            return GroupAccessor(self.og, self.og, key="/", fs=self.fs)

        filepath: PathLike
        fileformat: str | None = None
        memo: dict[str, Any] | None = None

        # any contents to pass?
        try:
            memo = self.contents[key]
            # if there are more data for this key,
            # should return a dictionary
        except (KeyError, TypeError):
            memo = {}
        # is there more data?
        for suffix in "json", "yaml", "yml", "toml":
            if self.fs.exists(textfile := self.path / f"{key}.{suffix}"):
                # FIXME
                memo |= read_textfile(textfile)
                fileformat = suffix

        # directory?
        filepath = self.path / key
        if self.fs.exists(filepath):
            return GroupAccessor(filepath, self.og, key=key, memo=memo, fs=self.fs)

        # file?
        for suffix in ("pq", "parquet"):
            filepath = self.path / f"{key}.{suffix}"
            if self.fs.exists(filepath):
                fileformat = suffix
                break

        # content?
        if self.contents:
            try:
                return self.contents[key]
            except KeyError:
                pass

        if fileformat is None:
            raise KeyError(f"Key {key} not found in {self.path}")
        elif fileformat in ("parquet", "pq"):
            return Array(filepath, fileformat, root=self.name, key=key, fs=self.fs)
        else:
            return GroupContents(filepath, fileformat, key=key, fs=self.fs)

    def __repr__(self):
        return f"ParquetStorage({self.path})"

    def __len__(self):
        return len(self.fs.listdir(self.path, detail=False))

    def has_key(self, k):
        return self.__contains__(k)

    def keys(self):
        os_keys = self.fs.listdir(self.path, detail=False)
        os_keys = [Path(key).stem for key in os_keys]
        memo_keys = list(self.contents.keys()) if self.contents else []
        all_keys = set(os_keys) | set(memo_keys)

        # Exceptions for the root
        exceptions: tuple[str] = ()
        if self.path.suffix == ".pqdata":
            exceptions = ("pqdata.json", "pqdata.yaml", "pqdata.toml")

        used_keys = []
        for key in all_keys:
            if key in exceptions:
                continue
            if key.endswith(SUPPORTED_EXTENSIONS):
                key = Path(key).stem
            if key in used_keys:
                continue
            used_keys.append(key)
            yield key

    def values(self):
        raise NotImplementedError("values() not implemented.")

    def items(self):
        raise NotImplementedError("items() not implemented.")

    def __contains__(self, item):
        path = self.path / item
        exists = path.exists()  # true only for directories

        if not exists:
            for suffix in SUPPORTED_EXTENSIONS:
                path = self.path / f"{item}.{suffix}"
                exists = path.exists()
                if exists:
                    break

        return exists

    def __iter__(self):
        return iter(self.keys())


def read_textfile(path: PathLike):
    path = Path(path)
    if path.suffix == ".json":
        import json as lib
    elif path.suffix in (".yaml", ".yml"):
        import yaml as lib
    elif path.suffix == ".toml":
        import toml as lib
    else:
        raise NotImplementedError(f"File format {path.suffix} not supported.")

    with path.open() as file:
        return lib.load(file)


class GroupContents(Group):
    """
    Container for contents read from a json file.
    """

    def __init__(self, path: PathLike, fileformat: str, key: str = ""):
        super().__init__(path)
        self.name = str(Path(self.name) / key)
        self.contents = None

        if fileformat == "json":
            import json

            self.contents = json.load(self.path)
        elif fileformat in ("yaml", "yml"):
            import yaml

            self.contents = yaml.load(self.path)
        elif fileformat == "toml":
            import toml

            self.contents = toml.load(self.path)
        else:
            raise NotImplementedError(f"File format {fileformat} not supported.")

    def __getitem__(self, key: str):
        try:
            self.contents[key]
        except KeyError:
            raise NotImplementedError(f"Key {key} not found in {self.path}")


class Array:
    """
    HDF5/Zarr-like interface to access a single object
    stored in a parquet or a json file.
    """

    def __init__(self, path: PathLike, fileformat: str, root: str = "/", key: str = "", fs: AbstractFileSystem | None = None):
        self.path = Path(path)
        self.fileformat = fileformat
        self.root = root
        self.name = str(Path(root) / key)
        self.fs = fs

        if not self.fs.exists(self.path):
            raise FileNotFoundError(f"Path {self.path} does not exist.")

        with self.fs.open(self.path) as file:
            meta = pq.read_metadata(file)
        r, c = meta.num_rows, meta.num_columns
        self.shape = (r, c)
        # FIXME: not true for sparse matrices

    def __getitem__(self, key: Any):
        with self.fs.open(self.path) as file:
            table = pq.read_table(file)
        return table.__getitem__(key)

    def __repr__(self) -> str:
        with self.fs.open(self.path) as file:
            schema = pq.read_schema(file)
        c_types = ", ".join([f"{c.name}:{c.type}" for c in schema])
        s = f"ParquetArray({self.path}): shape ({','.join(str(e) for e in self.shape)}), type ({c_types})"
        return s

    @property
    def attrs(self):
        return ()


def read_elem(elem: Array):
    with elem.fs.open(elem.path) as file:
        table = read_table(file)
    return table


def open_storage(
    path: PathLike,
    mode: str = "r",
):
    # TODO: modes
    return GroupAccessor(path)
