import itertools
import logging
import shutil
import tempfile
from functools import partial
from typing import Dict, Any, Type, Optional

import pytest
import statey as st


LOGGER = logging.getLogger(__name__)


def data_container_resource(
    name: str, attrs: Optional[Dict[str, Any]] = None
) -> Type[st.Resource]:
    if attrs is None:
        attrs = {}

    fields = {}
    for field_name, annotation in attrs.items():
        field = annotation
        if isinstance(annotation, st.Field):
            field = annotation
        elif isinstance(annotation, type) and issubclass(annotation, st.Field):
            field = annotation()
        else:
            field = st.Field[annotation]
        fields[field_name] = field

    schema_cls = type("Schema", (st.Resource.Schema,), fields)

    async def nothing(*args, **kwargs):
        pass

    return type(
        name,
        (st.Resource,),
        {
            "type_name": name,
            "Schema": schema_cls,
            "create": nothing,
            "destroy": nothing,
            "refresh": nothing,
            "update": nothing,
        },
    )


@pytest.fixture(autouse=True)
def clear_type_cache():
    previous = type(st.Resource)._type_cache.copy()
    yield
    type(st.Resource)._type_cache = previous


@pytest.fixture
async def state():
    with tempfile.NamedTemporaryFile() as ntf:
        state = st.State(ntf.name)
        yield state
        # Attempt to clean up state when we're done
        empty_graph = state.graph()
        plan = await state.plan(empty_graph)
        result = await state.apply(plan)
        assert (
            result.success
        ), f"Failed to clean up resources from state {state}: {result}"


@pytest.fixture
def graph(state):
    return state.graph()


@pytest.fixture
def tmpdir():
    dirname = tempfile.mkdtemp()
    yield dirname
    shutil.rmtree(dirname)
