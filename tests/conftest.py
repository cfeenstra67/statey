import pytest
import statey as st


@pytest.fixture
def session():
    return st.create_session()


@pytest.fixture
def registry(session):
    return session.ns.registry
