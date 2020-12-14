import shutil
import tempfile

import pytest
import statey as st

from tests.resources import register as register_test_resources

register_test_resources()


@pytest.fixture
def session():
    return st.create_session()


@pytest.fixture
def resource_session():
    return st.create_resource_session()


@pytest.fixture
def executor():
    return st.AsyncIOGraphExecutor()


@pytest.fixture
def migrator():
    return st.DefaultMigrator()


@pytest.fixture
def registry(session):
    return st.registry


@pytest.fixture
def tmpdir():
    value = tempfile.mkdtemp()
    try:
        yield value
    finally:
        shutil.rmtree(value)
