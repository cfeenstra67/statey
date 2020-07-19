# import dataclasses as dc
# from typing import Type as PyType, Any, Optional, Dict


# @st.model
# class MyModel:
# 	"""

# 	"""
# 	a: int
# 	b: st.S.Struct["a": st.S.String]

# 	@st.method
# 	def method_one(self) -> int:
# 		"""
# 		blah blah
# 		"""
# 		return self

# 	@st.method(symbolic=True, property=True)
# 	def symbolic_method_one(obj):
# 		return obj.method_one() + obj.method_two()


# def model(cls: PyType[Any], dataclass_kwargs: Optional[Dict[str, Any]] = None) -> PyType[Any]:
# 	"""
# 	Wrap the given class to be a statey model whose hooks can be registered via
# 	registry.pm.register(model_cls)
# 	"""
# 	if dataclass_kwargs is None:
# 		dataclass_kwargs = {}

# 	dataclass_cls = dc.dataclass(cls, frozen=True, **dataclass_kwargs)






