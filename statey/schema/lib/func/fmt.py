"""
Common functions for use in graph generation
"""
import itertools
import sys
from functools import partial
from typing import Optional, Dict, Any

from fmt.fmt import Parser, Expression, Constant, generate

import statey as st


def _eval_name(name: str, expr: str, namespace: Dict[str, Any]):
    new_ns = {}
    try:
        # pylint: disable=exec-used
        exec(f"{name}={expr}", namespace.copy(), new_ns)
    except NameError as exc:
        name = exc.args[0].split("'", 3)[1]
        raise NameError(f'"{name}" cannot be found in any namespaces.') from exc
    return new_ns[name]


def fmt(template: str, scope: Optional[Dict[str, Any]] = None) -> st.Func:
    """
	Interpolation operator for fields

	E.g.
	resource_1 = SomeResource(a=1, ...)
	resource_2 = OtherResource(some_field=st.func.fmt('{resource_1.attrs.a} is the value.'), ...)
	"""
    variables = {} if scope is None else scope.copy()
    frame = sys._getframe(1)  # pylint: disable=protected-access
    variables.update(frame.f_globals)
    variables.update(frame.f_locals)

    nodes = Parser(template).parse()

    _next = partial(next, itertools.count(1))
    next_param = lambda: f"param_{_next()}"

    out_scope = {}
    out_nodes = []

    for node in nodes:
        if isinstance(node, Expression):
            # pylint: disable=protected-access,no-member
            obj = _eval_name(node._name, node._expr, variables)
        elif isinstance(node, Constant):
            obj = _eval_name(node._name, node._name, variables)  # pylint: disable=protected-access
        else:
            out_nodes.append(node)
            continue

        node._name = param_name = next_param()  # pylint: disable=protected-access
        out_scope[param_name] = obj
        out_nodes.append(Constant(param_name, "{" + param_name + "}"))

    def render_nodes(**kwargs):
        return generate(out_nodes, kwargs)

    return st.Func[str](render_nodes)(**out_scope)
