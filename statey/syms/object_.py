import abc
import dataclasses as dc
from functools import partial
from typing import Any, Dict, Sequence, Iterable, Optional, Callable, Union

import networkx as nx

from statey.syms import base, stack


def get_item_passthrough(name: str) -> Callable[[Any], Any]:
    """
	Get a simple method that will just get an attribute using __getitem__
	"""

    def func(self):
        return self[name]

    func.__name__ = name
    return property(func)


@dc.dataclass(frozen=True, repr=False)
class Object(base.Proxy):
    """
	A uniform API for structuring access to statey objects. Should not be
	called directly, use an API method to create objects
	"""

    impl: dc.InitVar[Any]
    type: dc.InitVar[Optional[Union["Type", Any]]] = None
    registry: dc.InitVar[Optional["Registry"]] = None
    frame: dc.InitVar[Optional[stack.FrameSnapshot]] = None
    _impl: "ObjectImplementation" = dc.field(init=False, default=None)
    _type: "Type" = dc.field(init=False, default=None)
    _registry: "Registry" = dc.field(init=False, default=None)
    _inst: "Proxy" = dc.field(init=False, default=None)
    _frame: stack.FrameSnapshot = dc.field(init=False, default=None)

    def __class_getitem__(cls, item: Any) -> "Object":
        """
        Create a partial object w/ the given annotation
        """
        return partial(cls, type=item)

    def __post_init__(
        self,
        impl: Any,
        type: Optional["Type"],
        registry: Optional["Registry"],
        frame: Optional[stack.FrameSnapshot],
    ) -> None:
        """
		Set up the Object instance
		"""
        import statey as st
        from statey.syms.impl import ObjectImplementation

        if isinstance(impl, Object):
            obj = impl
            impl = obj._impl
            type = type or obj._type
            registry = registry or obj._registry
            frame = frame or obj._frame

        elif not isinstance(impl, ObjectImplementation):
            if registry is None:
                registry = st.registry
            obj = registry.get_object(impl)
            impl = obj._impl
            type = type or obj._type

        if registry is None:
            try:
                registry = impl.registry()
            except NotImplementedError:
                pass

        if registry is None:
            registry = st.registry

        if type is not None and not isinstance(type, st.Type):
            # Convert an annotation to a type
            type = registry.get_type(type)

        if type is None:
            try:
                type = impl.type()
            except NotImplementedError:
                pass

        if type is None:
            raise ValueError(
                f"Object implementation {impl} does not have an implied type, one must "
                f"be passed manually."
            )

        if frame is None:
            frame = stack.frame_snapshot(2)

        self.__dict__["_impl"] = impl
        self.__dict__["_type"] = type
        self.__dict__["_registry"] = registry
        self.__dict__["_frame"] = frame

        impl_accessor = base.GetattrBasedAttributeAccess(self._impl)
        self.__dict__["_inst"] = base.BoundAttributeAccess(self, impl_accessor)

    @property
    def _accessor(self) -> base.AttributeAccess:
        methods = self._registry.get_methods(self._type)
        return base.OrderedAttributeAccess((self._impl, methods))

    @property
    def _instance(self) -> Any:
        return self

    def __repr__(self) -> str:
        """
		Produce a string representation of this object
		"""
        return self._inst.object_repr()

    @property
    def i(self) -> base.Proxy:
        """
		Convenient access to get an implementation-specific bound attribute accessor
		"""
        return base.BoundAttributeAccess(self, self._impl)

    @property
    def m(self) -> base.Proxy:
        """
		Convenient access for object methods
		"""
        return base.BoundAttributeAccess(self, self._registry.get_methods(self._type))

    @property
    def _(self) -> Any:
        """
		If this object can be trivially resolved alone, do that here. Otherwise, just
		return self. This is useful to resolve something "as much as possible" without
		having to reason about whether something is an Object vs. a regular Python object
		"""
        obj = self
        while isinstance(obj, Object):
            try:
                obj = obj._inst.apply_alone()
            except NotImplementedError:
                return obj
        return obj

    # Magic methods need to be defined on the type itself
    # http://docs.python.org/3/reference/datamodel.html#special-method-lookup
    __call__ = get_item_passthrough("__call__")

    __add__ = get_item_passthrough("__add__")

    __sub__ = get_item_passthrough("__sub__")

    __mul__ = get_item_passthrough("__mul__")

    __floordiv__ = get_item_passthrough("__floordiv__")

    __truediv__ = get_item_passthrough("__truediv__")

    __mod__ = get_item_passthrough("__mod__")

    __pow__ = get_item_passthrough("__pow__")

    __lshift__ = get_item_passthrough("__lshift__")

    __rshift__ = get_item_passthrough("__rshift__")

    __and__ = get_item_passthrough("__and__")

    __xor__ = get_item_passthrough("__xor__")

    __or__ = get_item_passthrough("__or__")

    __lt__ = get_item_passthrough("__lt__")

    __le__ = get_item_passthrough("__le__")

    __eq__ = get_item_passthrough("__eq__")

    __ne__ = get_item_passthrough("__ne__")

    __ge__ = get_item_passthrough("__ge__")

    __gt__ = get_item_passthrough("__gt__")

    __neg__ = get_item_passthrough("__ne__")

    __pos__ = get_item_passthrough("__pos__")

    __abs__ = get_item_passthrough("__abs__")

    __invert__ = get_item_passthrough("__invert__")

    __complex__ = get_item_passthrough("__complex__")

    __int__ = get_item_passthrough("__int__")

    __long__ = get_item_passthrough("__long__")

    __float__ = get_item_passthrough("__float__")

    __oct__ = get_item_passthrough("__oct__")

    __hex__ = get_item_passthrough("__hex__")
