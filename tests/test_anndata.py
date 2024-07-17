import numpy as np
import pandas as pd
import pytest
from anndata import AnnData
from scipy.sparse import coo_matrix

from pqdata.io.read import read_anndata
from pqdata.io.write import write_anndata

N, D = 50, 20


@pytest.fixture()
def adata(sparse_x: bool = False, obsm: str | None = None):
    np.random.seed(100)
    if sparse_x:
        sparsity = 0.2
        row = np.random.choice(N, 1000 * sparsity)
        col = np.random.choice(D, 1000 * sparsity)
        data = np.random.normal(size=1000 * sparsity)

        x = coo_matrix((data, (row, col)), shape=(N, D)).tocsr()
    else:
        x = np.random.normal(size=(N, D))

    ad = AnnData(X=x)

    if obsm:
        x2d = np.random.normal(size=(N, 2))
        if obsm == "pandas":
            x2d = pd.DataFrame(x2d)
        ad.obsm["X_2d"] = x2d

    return ad


@pytest.mark.usefixtures("filepath_ad")
class TestAnnData:
    @pytest.mark.parametrize("sparse_x", [True, False])
    @pytest.mark.parametrize("obsm", [None, "numpy", "pandas"])
    def test_anndata_sparse_matrix(self, adata, filepath_ad, sparse_x, obsm):
        write_anndata(adata, filepath_ad, overwrite=True)
        ad = read_anndata(filepath_ad)

        assert adata.shape == ad.shape
