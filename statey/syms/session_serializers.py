import abc
import dataclasses as dc
from typing import Any, Optional

import marshmallow as ma
import networkx as nx

import statey as st
from statey.syms import session, py_session, namespace_serializers as ns_serializers, impl


class SessionSerializer(abc.ABC):
    """
    Serializes a session object
    """
    def serialize(self, session: session.Session) -> Any:
        """
        Serialize the session to a JSON-serializable object
        """
        raise NotImplementedError

    def deserialize(self, data: Any) -> session.Session:
        """
        Deserialize the given data to a session object
        """
        raise NotImplementedError


class PythonSessionSchema(ma.Schema):
    """
    Schema for a python session dict
    """
    name = ma.fields.Str(required=True, default='python_session')
    graph = ma.fields.Dict()
    data = ma.fields.Dict()
    keys = ma.fields.Dict(keys=ma.fields.Str())
    ns = ma.fields.Dict()


@dc.dataclass(frozen=True)
class PythonSessionSerializer(SessionSerializer):
    """
    Serializer for a python session
    """
    ns_serializer: ns_serializers.NamespaceSerializer
    registry: "Registry"

    def serialize(self, session: session.Session) -> Any:
        serialized_ns = self.ns_serializer.serialize(session.ns)

        objects = {}
        for key, value in session.data.items():
            if isinstance(value, st.Object):
                objects[key] = value
            else:
                typ = session.ns.resolve(key)
                objects[key] = st.Object(impl.Data(value, typ))

        dag = nx.DiGraph()
        for key, obj in objects.items():
            session._build_symbol_dag(obj, dag)

        serialized_dag = nx.DiGraph()
        for node in dag.nodes:
            obj = dag.nodes[node]['symbol']
            obj_ser = self.registry.get_object_serializer(obj)
            serialized = obj_ser.serialize(obj)
            serialized_dag.add_node(node, object=serialized)

        serialized_dag.add_edges_from(dag.edges)
        graph_data = nx.to_dict_of_dicts(serialized_dag)

        node_data = {node: serialized_dag.nodes[node]['object'] for node in serialized_dag.nodes}
        obj_id_map = {key: obj._impl.id for key, obj in objects.items()}

        out = {
            'graph': graph_data,
            'data': node_data,
            'keys': obj_id_map,
            'ns': serialized_ns
        }
        return PythonSessionSchema().dump(out)

    def deserialize(self, data: Any) -> session.Session:
        loaded_data = PythonSessionSchema().load(data)

        ns = self.ns_serializer.deserialize(loaded_data['ns'])

        graph = nx.from_dict_of_dicts(loaded_data['graph'], create_using=nx.DiGraph)
        for key, value in loaded_data['data'].items():
            graph.nodes[key]['object'] = value

        objects = {}
        session = py_session.PythonSession(ns)

        for node in nx.topological_sort(graph):
            obj_data = graph.nodes[node]['object']
            obj_ser = self.registry.get_object_serializer_from_data(obj_data)
            obj = obj_ser.deserialize(obj_data, session, objects)
            objects[node] = obj

        for key, obj_id in loaded_data['keys'].items():
            session.set_data(key, objects[obj_id])

        return session

    @classmethod
    @st.hookimpl
    def get_session_serializer(cls, session: session.Session, registry: "Registry") -> SessionSerializer:
        if not isinstance(session, py_session.PythonSession):
            return None
        ns_serializer = registry.get_namespace_serializer(session.ns)
        return cls(ns_serializer, registry)

    @classmethod
    @st.hookimpl
    def get_session_serializer_from_data(cls, data: Any, registry: "Registry") -> SessionSerializer:
        if not isinstance(data, dict) or data.get('name') != 'python_session':
            return None
        ns_serializer = registry.get_namespace_serializer_from_data(data['ns'])
        return cls(ns_serializer, registry)


SESSION_SERIALIZER_CLASSES = [PythonSessionSerializer]


def register(registry: Optional["Registry"] = None) -> None:
    """
    Register session serializers classes with the given registry
    """
    if registry is None:
        registry = st.registry

    for cls in SESSION_SERIALIZER_CLASSES:
        registry.register(cls)
