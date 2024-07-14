import pytest

from pqdata.io.read import read_anndata
from pqdata.io.write import write_anndata

from scipy.sparse import coo_matrix
import numpy as np
import pandas as pd
import pyarrow as pa
from anndata import AnnData


@pytest.fixture()
def adata(sparse_x: bool = False):
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
    return ad


@pytest.mark.usefixtures("filepath_ad")
class TestAnnData:
    @pytest.mark.parametrize("sparse_x", [True, False])
    def test_anndata_sparse_matrix(self, adata, filepath_ad, sparse_x):
        write_anndata(adata, filepath_ad, overwrite=True)
        ad = read_anndata(filepath_ad)

        assert adata.shape == ad.shape
