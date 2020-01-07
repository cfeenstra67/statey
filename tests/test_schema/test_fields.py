from typing import Optional, Sequence, Dict, Set, Tuple, List

import pytest
import statey as st

from ..conftest import data_container_resource


@pytest.mark.parametrize(
    "annotation, name",
    [(int, "IntField"), (str, "StrField"), (float, "FloatField"), (bool, "BoolField")],
)
def test_field_annotation_get(annotation, name):
    from statey.schema.field import _FieldWithModifiers

    field = st.Field[annotation]
    assert isinstance(field, _FieldWithModifiers)
    assert field.field_cls.__name__ == name
    assert field.defaults == {"annotation": annotation}


def test_resolve_value(state, graph):
    Container = data_container_resource("Container", {"a": int, "b": int})
    resource = Container["some_name"](a=12, b=10123)
    graph.add(resource)

    assert resource.f.a.resolve(graph) == 12
    assert resource.f.b.resolve(graph) == 10123
    assert (resource.f.a + resource.f.b).resolve(graph) == 10135

    assert resource.snapshot.resolve(graph) == resource.schema_helper.snapshot_cls(
        a=12, b=10123
    )


def test_resolve_foreign_value_error(state, graph):
    Container1 = data_container_resource("Container1", {"a": str})
    Container2 = data_container_resource("Container2", {"some_name": int})

    container1 = Container1["container1"](a="blah")
    container2 = Container2["container2"](some_name=1153386)

    graph.add(container1)

    assert container1.f.a.resolve(graph) == "blah"

    try:
        container2.f.some_name.resolve(graph)
    except st.exc.InvalidReference as err:
        assert err.path == "Container2:/container2"
    else:
        assert False, "This should have raised an error"


def test_value_validation():
    Container = data_container_resource(
        "Container", {"a": str, "b": int, "c": Optional[bool]}
    )

    try:
        container = Container(a=None, b="abc", c="abc")
    except st.exc.InputValidationError as exc:
        assert exc.messages == {
            "a": ["Field may not be null."],
            "b": ["Not a valid integer."],
            "c": ["Not a valid boolean."],
        }
    else:
        assert False, "This should have raised an error."

    try:
        container = Container(a="blah", b=None)
    except st.exc.InputValidationError as exc:
        assert exc.messages == {"b": ["Field may not be null."]}
    else:
        assert False, "This should have raised an error"

    container = Container(a="This is a string", b=123, c=True)

    container = Container(a="This is another string", b=123)
    assert container.snapshot.c is None

    container = Container(a="This is yet another string.", b=12343142141, c=None)


def test_reserved_name():
    try:
        Container = data_container_resource("Container", {"__meta__": int})
    except st.exc.InitializationError as err:
        assert "Field name __meta__ in schema Schema is reserved." in str(err)
    else:
        assert False, "This should have raised an error."


def test_list_field():
    from statey.schema.field import ListField

    try:
        st.Field[Sequence]()
    except st.exc.InitializationError:
        pass
    else:
        assert False, "This should have raised an error."

    assert isinstance(st.Field[List[int]](), ListField)
    assert isinstance(st.Field[Sequence[int]](), ListField)

    Container = data_container_resource("Container", {"a": str, "b": Sequence[int]})
    container = Container(a="Some data.", b=[1, 2, 3, 4])

    try:
        container = Container(a="Some data.", b=["abc"])
    except st.exc.InputValidationError as exc:
        assert exc.messages == {"b": {0: ["Not a valid integer."]}}
    else:
        assert False, "This should have raised an error."


def test_dict_field():
    from statey.schema.field import DictField

    try:
        st.Field[Dict]()
    except st.exc.InitializationError:
        pass
    else:
        assert False, "This should have raised an error."

    assert isinstance(st.Field[Dict[str, int]](), DictField)
    assert isinstance(st.Field[Dict[int, List[str]]](), DictField)

    Container = data_container_resource("Container", {"a": Dict[int, int]})
    container = Container(a={1: 2, 3: 4})

    try:
        container = Container(a={"abc": 2})
    except st.exc.InputValidationError as exc:
        assert exc.messages == {"a": {"abc": {"key": ["Not a valid integer."]}}}
    else:
        assert False, "This should have raised an error."


def test_tuple_field():
    from statey.schema.field import TupleField

    try:
        st.Field[Tuple]()
    except st.exc.InitializationError:
        pass
    else:
        assert False, "This should have raised an error."

    assert isinstance(st.Field[Tuple[int, int, int]](), TupleField)
    assert isinstance(st.Field[Tuple[Tuple[int], int]](), TupleField)

    Container = data_container_resource("Container", {"a": Tuple[int, str]})
    container = Container(a=(1, "blah"))

    try:
        container = Container(a=("blalhdafea", "a"))
    except st.exc.InputValidationError as exc:
        assert exc.messages == {"a": {0: ["Not a valid integer."]}}
    else:
        assert False, "This should have raised an error."


def test_nested_field():
    from statey.schema.field import NestedField

    class TestSchema(st.Schema):
        a = st.Field[int]
        b = st.Field[bool]

    assert isinstance(st.Field[TestSchema](), NestedField)
    assert isinstance(st.Field[st.Schema](), NestedField)

    Container = data_container_resource("Container", {"a": TestSchema})
    container = Container(a={"a": 1, "b": False})

    try:
        container = Container(a={"a": "blah"})
    except st.exc.InputValidationError as exc:
        assert exc.messages == {
            "a": {
                "a": ["Not a valid integer."],
                "b": ["Missing data for required field."],
            }
        }
    else:
        assert False, "This should have raised an error."


def test_nested_field_reference(graph):
    async def nothing():
        pass

    class Dummy(st.Resource):
        type_name = "dummy"

        class Schema(st.Schema):
            a = st.Field[int]

            @st.schema.nested
            class nested_attr(st.Schema):
                attr_1 = st.Field[str]
                attr_2 = st.Field[bool]

        create = destroy = update = refresh = nothing

    dummy1 = Dummy(a=1, nested_attr={"attr_1": "blah", "attr_2": True})
    graph.add(dummy1)

    assert dummy1.f.a.resolve(graph) == 1
    assert dummy1.f.nested_attr.f.attr_1.resolve(graph) == "blah"
    assert dummy1.f.nested_attr.f.attr_2.resolve(graph) is True


def test_nested_field_decorator():
    class TestSchema(st.Schema):
        a = st.Field[int]

        @st.schema.nested
        class b(st.Schema):
            a = st.Field[str]
            b = st.Field[bool]

    helper = st.schema.SchemaHelper(TestSchema)
    assert isinstance(
        helper.load_input({"a": 1, "b": {"a": "blah", "b": False}}), helper.snapshot_cls
    )

    try:
        helper.load_input({"a": "blah", "b": {"a": "abc", "b": "def"}})
    except st.exc.InputValidationError as exc:
        assert exc.messages == {
            "a": ["Not a valid integer."],
            "b": {"b": ["Not a valid boolean."]},
        }
    else:
        assert False, "This should have raised an error."
