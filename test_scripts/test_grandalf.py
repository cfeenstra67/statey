from grandalf.graphs import Vertex, Edge, Graph


V = [Vertex(data) for data in range(10)]
E = [Edge(V[0], V[8])]
g = Graph(V, E)

from grandalf.layouts import SugiyamaLayout

class defaultview:
    w, h = 10, 10

for v in V: v.view = defaultview()

sug = SugiyamaLayout(g.C[0])
sug.init_all()
sug.draw()





