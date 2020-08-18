import dataclasses as dc
from typing import Optional, Sequence

from statey.syms.api import F, struct
from statey.syms.py_session import create_session


@struct
@dc.dataclass
class Test1:
	a: int
	b: str
	c: Optional[bool]


@struct
@dc.dataclass
class Test2:
	# a: object
	b: Sequence[Test1]
	c: Optional[str]
	d: Optional[Test1]


@struct
@dc.dataclass
class Test3:
	a: int
	b: str
	c: Optional[bool]


# class Resource:
# 	"""

# 	"""
# 	UP = State('DOWN', Type1)
# 	DOWN = State('UP')

# 	pass


# class ResourceState:
# 	"""

# 	"""
# 	state: State
# 	data: Any


# 	pass

# Register plugins
from statey.syms.ext import encoders, plugins
encoders.register()
plugins.register()

from statey import registry
print(registry.get_type(Test2))


session = create_session()

a = session['a'] << Test1(123, 'abc', None)

b = session['b': Test1] << {'a': 32, 'b': 'AAAAA', 'c': 'false'}

c = session['c'] << [Test1(a.a, b.b, True), Test1(b.a, a.b, None)]

e_obj = Test2(
	b=c,
	c=None,
	d=None
)
e = session['e'] << e_obj

print("YUCK!", e_obj)

# Equivalent to that one-liner!
# struct_type = StructType(
# 	[
# 		StructField('a', IntegerType(False)),
# 		StructField('b', StringType(False)),
# 		StructField('c', BooleanType(True))
# 	]
# )
# struct_type.pm.register(ParseDataClassPlugin(Test1))
# array_type = ArrayType(struct_type, False)
# c = session.ns.new('c', array_type) # = session.ns.new('c', registry.parse(Sequence[Test1]))
# session.set_data('c', [{'a': a.a, 'b': b.b, 'c': True}, {'a': b.a, 'b': a.b, 'c': None}])


d = session['d'] << F[int](lambda x, y: x + y)(a.a, b.a)

fib_a = session['fib_a'] << 0
fib_b = session['fib_b'] << 1


for _ in range(560):
	fib_a, fib_b = fib_b, fib_a + fib_b

print(e.type)

print('RESULT:', session.resolve(c))
print('RESULT?', session.resolve(b))
print("RESULT2", session.resolve(c.x.a))
print("WHATEVA", session.resolve(e))
ref = e.b[1:]
print("WHATEVA2", ref, session.resolve(ref))
sym = c[:][-1].a
print("PATH", sym.path)
print("RESULT3", session.resolve(sym))
print("RESULT4", repr(session.resolve(F[int](min)(d, -137))))

print("FIB TYPE", fib_b.type)
print("FIB", session.resolve(fib_b))
