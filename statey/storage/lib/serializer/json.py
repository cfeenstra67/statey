"""
A Serializer implementation that serializes graph to a JSON format.
"""
import json
from typing import Dict, Any, Optional

from statey import exc
from statey.resource import ResourceGraph, Resource
from statey.schema import SchemaHelper, Literal, Reference
from statey.storage import Serializer


class JSONSerializer(Serializer):
    """
	Default JSON serializer for state data
	"""

    # pylint: disable=too-many-locals
    @staticmethod
    def dump_to_dict(graph: ResourceGraph, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
		Dump the graph to a serializable dictionary.
		This is a custom format intended to be human-re
		"""
        nodes = {}

        dependencies = {}
        for src, dest, data in graph.graph.edges(data=True):
            dependencies.setdefault(dest, {}).setdefault(src, []).append(data)

        for node in graph.graph:
            depends_on = dependencies.get(node, {})
            data = graph.graph.nodes[node]
            if data["snapshot"] is None:
                nodes[node] = {
                    "name": data["resource"].name,
                    "type_name": data["resource"].type_name,
                    "snapshot": None,
                    "exists": False,
                    "dependencies": {},
                    "lazy": [],
                }
                continue

            snapshot, exists = data["snapshot"], data.get("exists", False)
            snapshot_data = snapshot and SchemaHelper(snapshot.source_schema).dump(snapshot)

            deps = {}
            for src, edges in depends_on.items():
                if src == node:
                    continue

                for edge in edges:
                    field_deps = deps.setdefault(edge["destination_field_name"], {}).setdefault(
                        src, set()
                    )
                    field_deps.add(edge["field"])

            deps = {k: {k2: sorted(v2) for k2, v2 in v.items()} for k, v in deps.items()}

            nodes[node] = {
                "name": data["resource"].name,
                "type_name": data["resource"].type_name,
                "snapshot": snapshot_data,
                "exists": exists,
                "dependencies": deps,
                "lazy": sorted(snapshot.__meta__.get("lazy", [])),
            }

        if meta is None:
            meta = {}

        return {"meta": meta, "resources": nodes}

    @staticmethod
    def _load_resource_from_dict(graph: ResourceGraph, path: str, data: Dict[str, Any]) -> None:
        resource_cls = Resource.find(data["type_name"])
        if resource_cls is None:
            raise exc.UndefinedResourceType(data["type_name"])

        schema_helper = SchemaHelper(resource_cls.Schema)

        snapshot_data = schema_helper.load(data["snapshot"])
        graph_data = {}

        for field, value in snapshot_data.items():
            if field not in data["dependencies"]:
                graph_data[field] = value
                continue

            refs = []
            for src_path, src_fields in data["dependencies"][field].items():
                for src_field in src_fields:
                    other = graph.query(src_path, False)
                    field_obj = other["resource_cls"].Schema.__fields__[src_field]

                    refs.append(Reference(resource=src_path, field_name=src_field, field=field_obj))

            field_obj = resource_cls.Schema.__fields__[field]
            graph_data[field] = Literal(value=value, type=field_obj, refs=tuple(refs))

        snapshot = schema_helper.snapshot_cls(**graph_data)
        snapshot.__meta__["lazy"] = data.get("lazy", [])
        resource = resource_cls.from_snapshot(snapshot)
        snapshot = snapshot.fill_missing_values(resource)

        graph.add(
            snapshot=snapshot,
            name=data["name"],
            exists=data["exists"],
            path=path,
            resource=resource,
            resource_cls=resource_cls,
        )

    def load_from_dict(self, data: Dict[str, Any], session: "Session") -> ResourceGraph:
        """
		Construct a ResourceGraph given a dictionary constructed vai dump_to_dict()
		"""
        not_processed = set(data["resources"])
        graph = ResourceGraph(session)

        while len(not_processed) > 0:

            for path in sorted(not_processed):
                resource = data["resources"][path]
                if set(resource["dependencies"]) & not_processed:
                    continue
                node = data["resources"][path]
                # If no state already exists we don't need to add it to the graph
                if node["snapshot"] is not None:
                    self._load_resource_from_dict(graph, path, node)
                not_processed.remove(path)

        return graph

    def dump(self, graph: ResourceGraph) -> bytes:
        serializable = self.dump_to_dict(graph)
        return json.dumps(serializable, indent=4, sort_keys=True).encode()

    def load(self, state_data: bytes, session: "Session") -> ResourceGraph:
        if state_data == b"":
            return ResourceGraph(session)
        serializable = json.loads(state_data.decode())
        loaded = self.load_from_dict(serializable, session)
        return loaded
