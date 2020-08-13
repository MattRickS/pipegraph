import constants


class Graph(object):
    def __init__(self):
        self._connections = set()
        self._nodes = set()

    def add_connection(self, connection):
        # Input ports only accept a single connection unless declared as "multi"
        # If a connection exists for a non-multi Input port, raise an error
        single_inputs = [
            p.type() == constants.PortType.Input and not p.is_multi()
            for p in (connection.source(), connection.target())
        ]
        if single_inputs:
            for con in self._connections:
                if any(p == con.source() or p == con.target() for p in single_inputs):
                    raise ValueError(
                        "Multiple connections for single-connection port: {}".format(
                            connection
                        )
                    )

        total = len(self._connections)
        self._connections.add(connection)
        return len(self._connections) != total

    def add_node(self, node):
        total = len(self._nodes)
        self._nodes.add(node)
        return len(self._nodes) != total

    def iter_connections(self):
        yield from self._connections

    def iter_nodes(self):
        yield from self._nodes

    def connected(self, port):
        for connection in self._connections:
            if connection.source() == port:
                yield connection.target()
            elif connection.target() == port:
                yield connection.source()

    def node(self, name, type=None):
        for node in self._nodes:
            if node.name() == name and (type is None or type == node.type()):
                return node


if __name__ == "__main__":
    import yaml
    import loader
    import nodes

    with open("/home/mshaw/git/python/pipegraph/config/graph.yml") as f:
        config = yaml.safe_load(f)

    loader = loader.ConfigLoader(config)

    root = nodes.Node("root", "pipeline")
    project = loader.create_stage_node("project", "project", {}, root)
    assetA = loader.create_stage_node("asset", "assetA", {}, project)
    assetB = loader.create_stage_node(
        "asset", "assetB", {"is_rigged": {"type": bool, "value": True}}, project
    )
    shot = loader.create_stage_node(
        "shot",
        "shotA",
        {"assets": {"type": "list", "value": [assetA, assetB]}},
        project,
    )

    def path(port):
        p = [port.name()]
        n = port.node()
        while n:
            p.append(n.name())
            n = n.parent()
        return ".".join(p[::-1])

    def format_connection(connection):
        return "{} -> {} | {}".format(
            path(connection.source()), path(connection.target()), connection.metadata
        )

    g = Graph()
    for node in (project, assetA, assetB, shot):
        g.add_node(node)
        for c in loader.create_connections(node):
            if not g.add_connection(c):
                print("Failed to add:", format_connection(c))

    print("=" * 80)
    for c in sorted(
        g._connections, key=lambda con: con.source().node().parent().name()
    ):
        print(format_connection(c))
    print("=" * 80)

    n = g.node("assetA")
    print(n)
    for port in g.connected(
        n.child("modeling").port(constants.PortType.Output, "model")
    ):
        print(path(port))
