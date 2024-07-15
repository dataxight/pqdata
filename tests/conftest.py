import pytest


@pytest.fixture(scope="module")
def filepath_ad(tmpdir_factory):
    yield str(tmpdir_factory.mktemp("tmp_test_dir_pqdata").join("test.pqdata"))


@pytest.fixture(scope="module")
def filepath_mu(tmpdir_factory):
    yield str(tmpdir_factory.mktemp("tmp_test_dir_pqdata").join("test.pqdata"))


@pytest.fixture(scope="module")
def filepath_h5ad(tmpdir_factory):
    yield str(tmpdir_factory.mktemp("tmp_test_dir_pqdata").join("test.h5ad"))
