import pytest
import statey as st

from tests.resources import register as register_test_resources

register_test_resources()


@pytest.fixture
def session():
    return st.create_session()


@pytest.fixture
def registry(session):
    return session.ns.registry
