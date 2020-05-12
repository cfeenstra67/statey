def test_context_scope():
    from statey.utils.helpers import context_scope

    with context_scope() as scope:
        a = 123
        b = 456

    assert scope == {"a": 123, "b": 456, "scope": scope}
