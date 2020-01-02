"""
Common functions for use in graph generation
"""
import sys
from typing import Optional, Dict, Any

from fmt.fmt import Parser, generate

import statey as st


__all__ = [
	'F',
	'f',
	'fmt'
]


def fmt(template: str, scope: Optional[Dict[str, Any]] = None) -> st.Func:
	"""
	Interpolation operator for fields

	E.g.
	resource_1 = SomeResource(a=1, ...)
	resource_2 = OtherResource(some_field=st.func.fmt('{resource_1.attrs.a} is the value.'), ...)
	"""
	variables = {} if scope is None else scope.copy()
	frame = sys._getframe(1)
	variables.update(frame.f_globals)
	variables.update(frame.f_locals)

	nodes = Parser(template).parse()

	def render_nodes(**kwargs):
		return generate(nodes, kwargs)

	return st.Func(render_nodes).partial(**variables)


# Shortcuts
f = fmt
F = st.Func
