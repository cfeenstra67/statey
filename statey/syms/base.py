"""
Base classes for basic functionality used in several different types of objects
"""
import abc
import dataclasses as dc
import sys
from functools import partial
from typing import Any, Sequence

from statey import exc
from statey.syms.stack import internalcode, rewrite_tb


class AttributeAccess(abc.ABC):
    """
	Defines some interface for getting attributes for some object
	"""

    @abc.abstractmethod
    def get_attr(self, obj: Any, attr: str) -> Any:
        """
		Get some attribute by name on the given object
		"""
        raise NotImplementedError


@dc.dataclass(frozen=True)
class OrderedAttributeAccess(AttributeAccess):
    """
	Combine a set of AttributeAccess instances and attempt to use them in order
	"""

    accessors: Sequence[AttributeAccess]

    @internalcode
    def get_attr(self, obj: Any, attr: str) -> Any:
        for accessor in self.accessors:
            try:
                return accessor.get_attr(obj, attr)
            except (exc.SymbolAttributeError, exc.SymbolKeyError):
                pass
        raise exc.SymbolAttributeError(obj, attr)


@dc.dataclass(frozen=True)
class GetattrBasedAttributeAccess(AttributeAccess):
    """
	Simple default python attribute access
	"""

    obj: Any

    @internalcode
    def get_attr(self, obj: Any, attr: str) -> Any:
        return partial(getattr(self.obj, attr), obj)


class Proxy(abc.ABC):
    """
	Defines a generic way to bind an attribute access object to some instance
	"""

    @property
    def _instance(self) -> Any:
        """
		Allow use of this class 
		"""
        return self

    @property
    @abc.abstractmethod
    def _accessor(self) -> AttributeAccess:
        """
		Get the AttributeAccess instance for this instance
		"""
        raise NotImplementedError

    @internalcode
    def __getitem__(self, attr: Any) -> Any:
        """
		Main API method for accessing attributes. Test all accessors in order.
		"""
        try:
            return self._accessor.get_attr(self._instance, attr)
        except Exception:
            rewrite_tb(*sys.exc_info())

    @internalcode
    def __getattr__(self, attr: Any) -> Any:
        try:
            return getattr(super(), attr)
        except AttributeError:
            pass
        try:
            return self[attr]
        except Exception:
            rewrite_tb(*sys.exc_info())


@dc.dataclass(frozen=True)
class BoundAttributeAccess(Proxy):
    """
	Simple object to allow attribute access on an arbitrary instance
	"""

    instance: dc.InitVar[Any]
    accessor: dc.InitVar[AttributeAccess]
    _instance: Any = dc.field(init=False, default=None)
    _accessor: AttributeAccess = dc.field(init=False, default=None)

    def __post_init__(self, obj: Any, accessor: AttributeAccess) -> None:
        self.__dict__["_instance"] = obj
        self.__dict__["_accessor"] = accessor
