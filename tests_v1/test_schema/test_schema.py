from typing import Optional

import marshmallow as ma
import statey as st


def test_create_schema():
    from statey.schema.field import IntField, BoolField, StrField
    from statey.schema.schema import SchemaMeta

    class TestSchema(st.Schema):
        field_a = st.Field[int]
        field_b = st.Field[bool]()
        field_c = st.Field[Optional[str]]

    assert isinstance(TestSchema, SchemaMeta)
    assert set(TestSchema.__fields__) == {"field_a", "field_b", "field_c"}

    assert TestSchema.field_a is TestSchema.__fields__["field_a"]
    assert isinstance(TestSchema.field_a, IntField)
    assert TestSchema.field_a.optional is False

    assert TestSchema.field_b is TestSchema.__fields__["field_b"]
    assert isinstance(TestSchema.field_b, BoolField)
    assert TestSchema.field_b.optional is False

    assert TestSchema.field_c is TestSchema.__fields__["field_c"]
    assert isinstance(TestSchema.field_c, StrField)
    assert TestSchema.field_c.optional is True


class test_create_schema_subclass:
    from statey.schema import field

    class ParentSchema(st.Schema):
        blah = st.Field[bool]
        blah2 = st.Field[float]

    class ChildSchema(ParentSchema):
        blah = None
        blah3 = st.Field[Optional[str]]

    assert set(ParentSchema.__fields__) == {"blah", "blah2"}
    assert set(ChildSchema.__fields__) == {"blah2", "blah3"}

    # assert ParentSchema.blah2 == ChildSchema.blah2 == ParentSchema.__fields__['blah2'] == ChildSchema.__fields__['blah2']
    assert isinstance(ParentSchema.blah2, field.FloatField)
    assert ParentSchema.blah2.optional is False

    assert ParentSchema.blah is ParentSchema.__fields__["blah"]
    assert isinstance(ParentSchema.blah, field.BoolField)
    assert ParentSchema.blah.optional is False

    assert ChildSchema.blah3 is ChildSchema.__fields__["blah3"]
    assert isinstance(ChildSchema.blah3, field.StrField)
    assert ChildSchema.blah3.optional is True


def test_schema_load():
    class TestSchema(st.Schema):
        foo = st.Field[bool](create_new=True)
        bar = st.Field[Optional[str]]
        blah = st.Field[int](computed=True)

    helper = st.schema.SchemaHelper(TestSchema)

    try:
        helper.load_input({"foo": True, "bar": "False", "blah": 123})
    except ma.ValidationError as exc:
        assert set(exc.messages) == {"blah"}
    else:
        assert False, "This should have raised an error"

    try:
        helper.load_input({"foo": True})
    except ma.ValidationError as exc:
        assert False, "This should not have raised in error"

    try:
        helper.load({"foo": True, "bar": "False", "blah": 123})
    except ma.ValidationError as exc:
        assert False, "This should not have raised an error"

    try:
        helper.load_input({"bar": "afoe"})
    except ma.ValidationError as exc:
        assert set(exc.messages) == {"foo"}
    else:
        assert False, "This should have raised an error"

    try:
        helper.load_input({"foo": "abc"})
    except ma.ValidationError as exc:
        assert set(exc.messages) == {"foo"}
    else:
        assert False, "This should have raised an error"


def test_schema_dump():
    class TestSchema(st.Schema):
        foo = st.Field[bool](create_new=True)
        bar = st.Field[Optional[str]]
        blah = st.Field[int](computed=True)

    helper = st.schema.SchemaHelper(TestSchema)

    assert helper.dump({"foo": True, "bar": "blahabc", "blah": 123}) == {
        "foo": True,
        "bar": "blahabc",
        "blah": 123,
    }


def test_resource_schema():
    async def nothing(*args, **kwargs):
        pass

    class Dummy(st.Resource):

        type_name = "dummy"

        class Schema(st.Resource.Schema):
            a = st.Field[int]
            b = st.Field[bool]

        create = destroy = refresh = update = nothing

    try:
        resource = Dummy(a="abc", b=False)
    except st.exc.InputValidationError as err:
        assert err.messages == {"a": ["Not a valid integer."]}
    else:
        assert False, "This should have raised an error."

    resource = Dummy(a=691932, b=True)

    assert resource.snapshot == resource.schema_helper.snapshot_cls(a=691932, b=True)


def test_schema_not_serializable():

    try:
        class Test(st.Schema):
            a = st.Field
    except st.exc.InvalidField as exc:
        assert 'Field "a"' in str(exc)
        assert 'is not serializable' in str(exc)
    else:
        assert False, 'This should have raised an error'


    class Test(st.Schema):
        a = st.Field(store=False)
