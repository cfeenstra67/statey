import json

import pytest
import statey as st
from statey.storage.lib.serializer.json import JSONSerializer

from ..conftest import data_container_resource


@pytest.mark.asyncio
async def test_json_serializer_dump(state, graph, tmpdir):
    Container = data_container_resource("Container", {"attr1": int, "attr2": str})

    serializer = JSONSerializer()
    container1 = Container["1"](attr1=123, attr2="ABCDEFG.")
    graph.add(container1)

    len_sym = st.F[int](len)
    container2 = Container["2"](
        attr1=len_sym(container1.f.attr2),
        attr2=container1.f.attr2 + container1.f.attr2,
    )
    graph.add(container2)
    container3 = Container["3"](
        attr1=len_sym(container2.f.attr2),
        attr2=container1.f.attr2 + "HELLO WORLD!",
    )
    graph.add(container3)

    resolved = graph.resolve_all()
    serialized = serializer.dump(resolved)

    assert json.loads(serialized) == {
        "meta": {},
        "resources": {
            "Container:/1": {
                "dependencies": {},
                "lazy": [],
                "exists": False,
                "snapshot": {"attr1": 123, "attr2": "ABCDEFG."},
                "name": "1",
                "type_name": "Container",
            },
            "Container:/2": {
                "dependencies": {
                    "attr1": {"Container:/1": ["attr2"]},
                    "attr2": {"Container:/1": ["attr2"]},
                },
                "lazy": [],
                "exists": False,
                "snapshot": {"attr1": 8, "attr2": "ABCDEFG.ABCDEFG."},
                "name": "2",
                "type_name": "Container",
            },
            "Container:/3": {
                "dependencies": {
                    "attr1": {"Container:/2": ["attr2"]},
                    "attr2": {"Container:/1": ["attr2"]},
                },
                "lazy": [],
                "exists": False,
                "snapshot": {"attr1": 16, "attr2": "ABCDEFG.HELLO WORLD!"},
                "name": "3",
                "type_name": "Container",
            },
        },
    }


@pytest.mark.asyncio
async def test_json_serializer_load(state, graph, tmpdir):
    Container = data_container_resource("Container", {"attr1": int, "attr2": str})
    schema_helper = st.schema.SchemaHelper(Container.Schema)
    snapshot_cls = schema_helper.snapshot_cls

    serializer = JSONSerializer()

    loaded = serializer.load(
        json.dumps(
            {
                "meta": {},
                "resources": {
                    "Container:/1": {
                        "dependencies": {},
                        "lazy": [],
                        "exists": False,
                        "snapshot": {"attr1": 123, "attr2": "ABCDEFG."},
                        "name": "1",
                        "type_name": "Container",
                    },
                    "Container:/2": {
                        "dependencies": {
                            "attr1": {"Container:/1": ["attr2"]},
                            "attr2": {"Container:/1": ["attr2"]},
                        },
                        "lazy": [],
                        "exists": False,
                        "snapshot": {"attr1": 8, "attr2": "ABCDEFG.ABCDEFG."},
                        "name": "2",
                        "type_name": "Container",
                    },
                    "Container:/3": {
                        "dependencies": {
                            "attr1": {"Container:/2": ["attr2"]},
                            "attr2": {"Container:/1": ["attr2"]},
                        },
                        "lazy": [],
                        "exists": False,
                        "snapshot": {"attr1": 16, "attr2": "ABCDEFG.HELLO WORLD!"},
                        "name": "3",
                        "type_name": "Container",
                    },
                },
            }
        ).encode(),
        state.registry,
    )

    assert set(loaded.graph) == {"Container:/1", "Container:/2", "Container:/3"}
    assert set(loaded.graph.pred["Container:/1"]) == set()
    assert set(loaded.graph.pred["Container:/2"]) == {"Container:/1"}
    assert set(loaded.graph.pred["Container:/3"]) == {"Container:/1", "Container:/2"}

    assert loaded.graph.nodes["Container:/1"]["snapshot"] == snapshot_cls(
        attr1=123, attr2="ABCDEFG."
    )
    assert loaded.graph.nodes["Container:/1"]["exists"] is False
    assert loaded.graph.nodes["Container:/2"]["snapshot"] == snapshot_cls(
        attr1=8, attr2="ABCDEFG.ABCDEFG."
    )
    assert loaded.graph.nodes["Container:/2"]["exists"] is False
    assert loaded.graph.nodes["Container:/3"]["snapshot"] == snapshot_cls(
        attr1=16, attr2="ABCDEFG.HELLO WORLD!"
    )
    assert loaded.graph.nodes["Container:/3"]["exists"] is False
