import pytest

from pqdata.io.read import read_anndata, read_mudata
from pqdata.io.write import write_anndata, write_mudata

from scipy.sparse import coo_matrix
import numpy as np
import pandas as pd
import pyarrow as pa
from anndata import AnnData
from mudata import MuData


@pytest.fixture()
def mdata(sparse_x: bool = False):
    np.random.seed(100)
    if sparse_x:
        sparsity = 0.2
        row = np.random.choice(50, 1000 * sparsity)
        col = np.random.choice(20, 1000 * sparsity)
        data = np.random.normal(size=1000 * sparsity)

        x = coo_matrix((data, (row, col)), shape=(50, 20)).tocsr()
    else:
        x = np.random.normal(size=(50, 20))
    ad = AnnData(X=x)
    ad2 = AnnData(X=x[:20, :10].copy())
    md = MuData({"mod1": ad, "mod2": ad2})
    md.var_names_make_unique()
    return md


@pytest.mark.usefixtures("filepath_mu")
class TestAnnData:
    @pytest.mark.parametrize("sparse_x", [True, False])
    def test_anndata_sparse_matrix(self, mdata, filepath_mu, sparse_x):
        write_mudata(mdata, filepath_mu, overwrite=True)
        md = read_mudata(filepath_mu)

        assert mdata.shape == md.shape
