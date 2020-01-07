import pytest
import statey as st

from ..conftest import data_container_resource


@pytest.mark.parametrize(
    "annotation, value, result", [(str, 1, "1"), (int, "12342", 12342), (bool, "some", True)]
)
def test_graph_convert(annotation, value, result):
    assert st.F[[annotation]](value).resolve(None) == result


@pytest.mark.parametrize(
    "func, result",
    [
        (st.F[int](lambda x, y: x // y)(5, 2), 2),
        (st.F[str](",".join)(["a", "b", "c"]), "a,b,c"),
        (st.F[bool](lambda x, y: x == y)(12, 12), True),
    ],
)
def test_graph_func(func, result):
    assert func.resolve(None) == result


def test_graph_func_resolve(graph):
    Collection = data_container_resource("Collection", {"a": int, "b": int})

    collection1 = Collection["a"](a=132, b=215)
    graph.add(collection1)
    collection2 = Collection["b"](a=123, b=6423)
    graph.add(collection2)

    sym = st.Func[int](lambda x, y, u, v: (x + u) * (y + v))(
        collection1.attrs.a, collection1.attrs.b, collection2.attrs.a, collection2.attrs.b
    )
    assert sym.resolve(graph) == 1692690


def test_graph_func_resolve_multiple(graph):
    Collection = data_container_resource("Collection", {"a": int, "b": int})

    collection = Collection["fib_1"](a=0, b=1)
    graph.add(collection)

    for i in range(2, 10):
        collection = Collection[f"fib_{i}"](
            a=collection.attrs.b, b=collection.attrs.a + collection.attrs.b
        )
        graph.add(collection)

    assert collection.attrs.b.resolve(graph) == 34

    fib = st.F[int](lambda x: (x[1], x[0] + x[1]))((0, 1))
    for _ in range(3, 10):
        fib = fib(fib)

    assert fib.resolve(None)[1] == 34


def test_graph_resolve_func_snapshot(graph):
    Collection = data_container_resource("Collection", {"a": int, "b": int})

    collection1 = Collection["a"](a=123, b=456)
    collection2 = Collection["b"](a=collection1.attrs.a - collection1.attrs.b, b=0)
    collection3 = Collection["c"](
        a=collection1.attrs.a, b=collection2.attrs.a + collection2.attrs.b
    )
    graph.add(collection1)
    graph.add(collection2)
    graph.add(collection3)

    assert collection3.snapshot.resolve(graph) == collection3.schema_helper.snapshot_cls(
        a=123, b=-333
    )


def test_graph_circular_reference(graph):
    async def nothing(*args, **kwargs):
        pass

    class Dummy(st.Resource):
        type_name = "dummy"

        class Schema(st.Resource.Schema):
            a = st.Field[int](optional=True, factory=lambda resource: resource.attrs.b)
            b = st.Field[int](optional=True, factory=lambda resource: resource.attrs.a)

        create = destroy = update = refresh = nothing

    resource1 = Dummy["a"]()
    graph.add(resource1)
    try:
        resource1.snapshot.resolve(graph)
    except st.exc.CircularReferenceDetected as exc:
        assert len(exc.nodes) == 3
    else:
        assert False, "This should have raised an error"

    resource2 = Dummy["b"](a=1)
    graph.add(resource2)
    assert resource2.snapshot.resolve(graph) == resource2.schema_helper.snapshot_cls(a=1, b=1)


def test_graph_circular_func_reference(graph):
    async def nothing(*args, **kwargs):
        pass

    class Dummy(st.Resource):
        type_name = "dummy"

        class Schema(st.Resource.Schema):
            a = st.Field[str](optional=True, factory=lambda resource: resource.attrs.c)
            b = st.Field[str](optional=True, factory=lambda resource: st.F[[str]](resource.attrs.a))
            c = st.Field[str](
                optional=True, factory=lambda resource: st.F[str](",".join)(resource.attrs.b)
            )

        create = destroy = update = refresh = nothing

    resource1 = Dummy["a"]()
    graph.add(resource1)

    try:
        resource1.snapshot.resolve(graph)
    except st.exc.CircularReferenceDetected as exc:
        assert len(exc.nodes) == 6
    else:
        assert False, "This should have raised an error"

    resource2 = Dummy["b"](a="abc")
    graph.add(resource2)

    assert resource2.snapshot.resolve(graph) == resource2.schema_helper.snapshot_cls(
        a="abc", b="abc", c="a,b,c"
    )


def test_graph_resolve_f_string(graph):
    Container = data_container_resource("Container", {"a": int, "b": str})

    container = Container["a"](a=1, b="blah")
    graph.add(container)
    value = st.f(
        """
	a: {container.attrs.a}
	b: {container.attrs.b}
	c: {[container.attrs.a for _ in range(3)]}
	d: {st.F[[str]](container.attrs.a) + container.attrs.b}
	"""
    )
    assert (
        value.resolve(graph)
        == """
	a: 1
	b: blah
	c: [1, 1, 1]
	d: 1blah
	"""
    )


def test_graph_resolve_operator(graph):
    Container = data_container_resource("Container", {"a": int, "b": int})

    container1 = Container["a"](a=3, b=7)
    graph.add(container1)
    container2 = Container["b"](a=2, b=23)
    graph.add(container2)

    value = container1.attrs.a * container2.attrs.b - container1.attrs.b
    assert value.resolve(graph) == 62
