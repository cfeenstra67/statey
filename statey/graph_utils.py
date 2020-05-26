import networkx as nx


def print_graph(graph: nx.DiGraph) -> None:
	print("Nodes:")
	for node in graph.nodes:
		print(f"{node}: {graph.nodes[node]}")

	print()
	print("Edges:")
	for tup in graph.edges:
		if len(tup) == 3:
			left, right, idx = tup
			print(f"{left} -> {right}[{idx}]: {graph.edges[left, right, idx]}")
		else:
			left, right = tup
			print(f"{left} -> {right}: {graph.edges[left, right]}")
