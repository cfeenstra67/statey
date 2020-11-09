from functools import wraps

import networkx as nx
import pytest
import statey as st


def factorial_func(n):

    def factorial(session):
        a = session['0'] << 0
        b = session['1'] << 1

        for i in range(2, n):
            new_val = session[str(i)] << a + b
            a, b = b, new_val

    return factorial


def factorial_func_deps(n):
    out = set()
    for i in range(2, n):
        out.add((str(i - 1), str(i)))
        out.add((str(i - 2), str(i)))

    return out


def set_items(scope_func):

    @wraps(scope_func)
    def wrapped(session):

        def ref(path, typ):
            return st.Object(st.Reference(path, session.ns), typ)

        scope = scope_func(session, ref=ref)
        for key, val in scope.items():
            session[key] << val

    return wrapped


@pytest.mark.parametrize('scope, result_key, result, edges', [
    pytest.param(
        set_items(lambda session, ref: {
            'a': 1,
            'b': ref('a', st.Integer) + 1
        }),
        'b', 2, {('a', 'b')},
        id='ref_int_1'
    ),
    pytest.param(
        set_items(lambda session, ref: {
            'a': 1,
            'b': ref('a', st.Integer) + 1,
            'c': ref('b', st.Integer) * 90
        }),
        'c', 180, {('a', 'b'), ('b', 'c')},
        id='ref_int_2'
    ),
    pytest.param(
        factorial_func(10),
        '9', 34, factorial_func_deps(10),
        id='factorial_10'
    ),
    # This is slow :( TODO: figure out why
    # pytest.param(
    #     factorial_func(100),
    #     '99', 218922995834555169026, factorial_func_deps(100),
    #     id='factorial_100'
    # ),
    pytest.param(
        set_items(lambda session, ref: {
            'a': 123,
            'b': {'a': ref('a', st.Integer), 'other_attr': 'Blah'},
            'c': ref('b.other_attr', st.String) + ' and other things.',
            'd': ref('c', st.String) + ' ' + st.str(ref('b.a', st.Integer))
        }),
        'd', 'Blah and other things. 123', {('a', 'b'), ('b', 'c'), ('c', 'd'), ('b', 'd')},
        id='ref_struct_1'
    ),
    pytest.param(
        set_items(lambda session, ref: {
            'a': 123,
            'b': {'a': ref('a', st.Integer), 'other_attr': 'Blah'},
            'c': {'c': ref('b.other_attr', st.Integer), 'd': st.str(ref('b.a', st.Integer))},
            'd': ref('c.c', st.String) + ref('c.d', st.String)
        }),
        'd', 'Blah123', {('a', 'b'), ('b', 'c'), ('c', 'd')},
        id='ref_struct_2'
    ),
    pytest.param(
        set_items(lambda session, ref: {
            'a': 123,
            'b': {'a': {'b': {'c': ref('a', st.Integer)}}, 'other_attr': 'Blah'},
            'c': ref('b.other_attr', st.String) + ' and other things.',
            'd': ref('c', st.String) + ' ' + st.str(ref('b.a.b.c', st.Integer))
        }),
        'd', 'Blah and other things. 123', {('a', 'b'), ('b', 'c'), ('c', 'd'), ('b', 'd')},
        id='ref_struct_3'
    ),
])
def test_session(session, scope, result_key, result, edges):
    scope(session)
    assert session.resolve(session.ns.ref(result_key)) == result
    graph = session.dependency_graph()
    edges_set = {(from_node, to_node) for from_node, to_node, _ in graph.edges}
    assert edges_set == edges
