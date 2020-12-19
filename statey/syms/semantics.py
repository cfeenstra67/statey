import abc
import copy
import dataclasses as dc
from typing import Any, Optional, Callable, Dict, Sequence

import statey as st
from statey.syms import types, utils, Object


class Semantics(abc.ABC):
    """
    Semantics define how we should treat a type within our symbolic API e.g.
    what type does an attribute of an instance of a certain type have?
    """

    type: types.Type

    @abc.abstractmethod
    def attr_semantics(self, attr: Any) -> Optional["Semantics"]:
        """
        Return the semantics of the given attribute of this object, if any. A return
        value of None indicates that `attr` is not an attribute of this object.
        """
        raise NotImplementedError

    def path_semantics(self, path: Sequence[Any]) -> Any:
        """
        Apply attr_semantics() multiple times
        """
        base_semantics = self
        for attr in path:
            base_semantics = base_semantics.attr_semantics(attr)
            if base_semantics is None:
                raise AttributeError(path)
        return base_semantics

    @abc.abstractmethod
    def get_attr(self, value: Any, attr: Any) -> Any:
        """
        Given a value, get the attribute value from an encoded value
        """
        raise NotImplementedError

    @abc.abstractmethod
    def map_objects(self, func: Callable[[Any], Any], value: Any) -> Any:
        """
        Assuming that `value` is value or symbol of this type, apply `func` to it
        (if it is an object) or any child objects otherwise
        """
        raise NotImplementedError

    @abc.abstractmethod
    def clone(self, value: Any) -> Any:
        """
        Return a semantically-correct deep copy of the given value
        """
        raise NotImplementedError

    @abc.abstractmethod
    def expand(self, value: Any) -> Any:
        """
        Given a possibly symbolic, expand it to be a dictionary (possibly containing
        objects)
        """
        raise NotImplementedError


@dc.dataclass(frozen=True)
class ValueSemantics(Semantics):
    """
    Semantics for regular values--these are the default semantics but structural types offer
    additional functionality.
    """

    type: types.Type

    def attr_semantics(self, attr: Any) -> Optional[Semantics]:
        return None

    def get_attr(self, value: Any, attr: Any) -> Any:
        raise st.exc.SymbolAttributeError(value, attr)

    def map_objects(self, func: Callable[[Any], Any], value: Any) -> Any:
        if isinstance(value, Object):
            return func(value)
        return value

    def expand(self, value: Any) -> Any:
        return value

    @classmethod
    @st.hookimpl
    def get_semantics(cls, type: types.Type, registry: st.Registry) -> Semantics:
        return cls(type)

    def clone(self, value: Any) -> Any:
        if isinstance(value, Object):
            return value.clone()
        return copy.copy(value)


@dc.dataclass(frozen=True)
class ArraySemantics(Semantics):
    """
    Semantics for ArrayTypes
    """

    type: types.Type
    element_semantics: Semantics

    def attr_semantics(self, attr: Any) -> Optional[Semantics]:
        if isinstance(attr, int):
            return self.element_semantics
        if isinstance(attr, slice):
            return self
        el_semantics = self.element_semantics.attr_semantics(attr)
        if el_semantics is not None:
            typ = types.ArrayType(el_semantics.type, self.type.nullable)
            return ArraySemantics(typ, el_semantics)
        return None

    def get_attr(self, value: Any, attr: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, Object):
            return value[attr]
        if isinstance(attr, (int, slice)):
            return value[attr]
        # "exploding"
        out = []
        for item in value:
            out.append(self.element_semantics.get_attr(item, attr))
        return out

    def map_objects(self, func: Callable[[Any], Any], value: Any) -> Any:
        if isinstance(value, Object):
            return func(value)
        if value is None:
            return None
        return [self.element_semantics.map_objects(func, item) for item in value]

    def clone(self, value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, Object):
            return value.clone()
        return [self.element_semantics.clone(item) for item in value]

    def expand(self, value: Any) -> Any:

        from statey.syms import api

        def func(x):
            if x is None:
                return None
            return [self.element_semantics.expand(val) for val in x]

        return api.map(func, value)

    @classmethod
    @st.hookimpl
    def get_semantics(cls, type: types.Type, registry: st.Registry) -> Semantics:
        if not isinstance(type, types.ArrayType):
            return None
        element_semantics = registry.get_semantics(type.element_type)
        return cls(type, element_semantics)


@dc.dataclass(frozen=True)
class MapSemantics(Semantics):
    """
    Semantics for MapTypes
    """

    type: types.Type
    key_semantics: Semantics
    key_encoder: "Encoder"
    value_semantics: Semantics

    def attr_semantics(self, attr: Any) -> Optional[Semantics]:
        try:
            encoded_attr = self.key_encoder.encode(attr)
        except st.exc.InputValidationError:
            return None
        return self.value_semantics

    def get_attr(self, value: Any, attr: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, Object):
            return value[attr]
        encoded_attr = self.key_encoder.encode(attr)
        try:
            return value[encoded_attr]
        except KeyError:
            return None

    def map_objects(self, func: Callable[[Any], Any], value: Any) -> Any:
        if isinstance(value, Object):
            return func(value)
        if value is None:
            return None
        return {
            self.key_semantics.map_objects(func, key): self.value_semantics.map_objects(
                func, val
            )
            for key, val in value.items()
        }

    def clone(self, value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, Object):
            return value.clone()
        return {
            self.key_semantics.clone(key): self.value_semantics.clone(val)
            for key, val in value.items()
        }

    def expand(self, value: Any) -> Any:

        from statey.syms import api

        def func(x):
            if x is None:
                return None
            return {
                self.key_semantics.expand(key): self.value_semantics.expand(val)
                for key, val in x.items()
            }

        return api.map(func, value)

    @classmethod
    @st.hookimpl
    def get_semantics(cls, type: types.Type, registry: st.Registry) -> Semantics:
        if not isinstance(type, types.MapType):
            return None
        key_semantics = registry.get_semantics(type.key_type)
        key_encoder = registry.get_encoder(type.key_type)
        value_semantics = registry.get_semantics(type.value_type)
        return cls(type, key_semantics, key_encoder, value_semantics)


@dc.dataclass(frozen=True)
class StructSemantics(Semantics):
    """
    Semantics for StructTypes
    """

    type: types.Type
    field_semantics: Dict[str, Semantics]

    def attr_semantics(self, attr: Any) -> Optional[Semantics]:
        if attr in self.field_semantics:
            return self.field_semantics[attr]
        return None

    def get_attr(self, value: Any, attr: Any) -> Any:
        if value is None:
            return None
        return value[attr]

    def map_objects(self, func: Callable[[Any], Any], value: Any) -> Any:
        if isinstance(value, Object):
            return func(value)
        if value is None:
            return None
        return {
            key: semantics.map_objects(func, value[key])
            for key, semantics in self.field_semantics.items()
        }

    def clone(self, value: Any) -> Any:
        if value is None:
            return None

        if isinstance(value, Object):
            return value.clone()
        out = {}
        for key, semantics in self.field_semantics.items():
            out[key] = semantics.clone(value[key])
        return out

    def expand(self, value: Any) -> Any:
        if not self.field_semantics or value is None:
            return value
        # If it is nullable it can't be expanded further
        if isinstance(value, Object) and self.type.nullable:
            return value

        out = {}
        for name, semantics in self.field_semantics.items():
            out[name] = semantics.expand(value[name])
        return out

    @classmethod
    @st.hookimpl
    def get_semantics(cls, type: types.Type, registry: st.Registry) -> Semantics:
        if not isinstance(type, types.StructType):
            return None
        field_semantics = {}
        for field in type.fields:
            field_semantics[field.name] = registry.get_semantics(field.type)
        return cls(type, field_semantics)


# Intentionally a list--this can be mutated if desired
SEMANTICS_CLASSES = [ValueSemantics, ArraySemantics, MapSemantics, StructSemantics]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register semantics plugins
    """
    if registry is None:
        registry = st.registry

    for cls in SEMANTICS_CLASSES:
        registry.register(cls)
