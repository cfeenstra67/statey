import pytest
import statey as st


@pytest.fixture
def session():
    return st.create_session()
