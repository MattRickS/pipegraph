import copy

import constants
import expression as exp_parser


class Port(object):
    def __init__(self, type, name, multi=False, metadata=None):
        self._type = type
        self._name = name
        self._node = None
        self._is_multi = multi
        self.metadata = metadata or {}

    def __getitem__(self, item):
        return self.metadata[item]["value"]

    def __repr__(self):
        return "{s.__class__.__name__}({s._type!r}, {s._name!r})".format(s=self)

    def name(self):
        return self._name

    def type(self):
        return self._type

    def is_multi(self):
        return self._is_multi

    def node(self):
        return self._node


class Node(object):
    def __init__(self, type, name, parent=None, metadata=None):
        self._type = type
        self._name = name
        self._parent = parent
        self.metadata = metadata or {}
        self._children = []
        self._ports = []

        if parent is not None:
            self.set_parent(parent)

    def __getitem__(self, item):
        return self.metadata[item]["value"]

    def __repr__(self):
        return "{s.__class__.__name__}({s._type!r}, {s._name!r})".format(s=self)

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.type() == other.type()
            and self.name() == other.name()
        )

    def __hash__(self):
        return hash((self.type(), self.name()))

    def name(self):
        return self._name

    def type(self):
        return self._type

    def child(self, name):
        for child in self._children:
            if child.name() == name:
                return child

    def children(self):
        return self._children[:]

    def parent(self):
        return self._parent

    def add_port(self, port):
        if port.node() is not None:
            raise ValueError("Port already belongs to a node")

        port._node = self
        self._ports.append(port)

    def port(self, type, name):
        for port in self._ports:
            if port.type() == type and port.name() == name:
                return port

    def ports(self):
        return self._ports[:]

    def set_parent(self, parent):
        parent._children.append(self)
        self._parent = parent


class Connection(object):
    def __init__(self, source, target, metadata=None):
        self._source = source
        self._target = target
        self.metadata = metadata or {}

    def __getitem__(self, item):
        return self.metadata[item]["value"]

    def __repr__(self):
        return "{}({!r}, {!r})".format(
            self.__class__.__name__, self.source(), self.target()
        )

    def __eq__(self, other):
        return (
            isinstance(other, Connection)
            and self.source() == other.source()
            and self.target() == other.target()
        )

    def __hash__(self):
        return hash((self.source(), self.target()))

    def source(self):
        return self._source

    def target(self):
        return self._target


class ConfigLoader(object):
    def __init__(self, config):
        self._config = config

    def _merge_metadata(self, metadata, data):
        d = copy.deepcopy(metadata)
        # TODO: Recursive merge
        d.update(data)
        return d

    def _resolve_conditional(self, conditional, keywords):
        condition_type = conditional["type"]
        if condition_type == "boolean":
            value = bool(exp_parser.parse(conditional["source"], keywords))
            return not value if conditional.get("invert") else value
        elif condition_type == "comparison":
            comparison = conditional["comparison"]
            source = exp_parser.parse(conditional["source"], keywords)
            target = exp_parser.parse(conditional["target"], keywords)
            if comparison == "in":
                return source in target
            else:
                raise ValueError(
                    "Unsupported comparison operator: {}".format(comparison)
                )
        else:
            raise ValueError("Unsupported conditional type: {}".format(condition_type))

    def _load_port(self, node, port_type, port_name, port_config):
        port = Port(
            port_type,
            port_name,
            multi=port_config.get("multi", False),
            metadata=port_config.get("data", {}),
        )
        node.add_port(port)
        return port

    def _load_workspace(self, workspace_node, workspace_name, workspace_data):
        workspace = Node(
            "workspace",
            workspace_name,
            parent=workspace_node,
            metadata=workspace_data.get("data", {}),
        )
        for port_type, port_name, port_config in self._iter_port_configs(
            workspace, workspace_data
        ):
            self._load_port(workspace, port_type, port_name, port_config)

    def _iter_port_configs(self, workspace_node, workspace_config):
        for input_name, input_data in workspace_config.get(
            constants.PortType.Input, {}
        ).items():
            yield constants.PortType.Input, input_name, input_data

        for output_name, output_data in workspace_config.get(
            constants.PortType.Output, {}
        ).items():
            yield constants.PortType.Output, output_name, output_data

        for condition_data in workspace_config.get("conditional", []):
            for conditional in condition_data["conditions"]:
                if not self._resolve_conditional(
                    conditional,
                    {"workspace": workspace_node, "stage": workspace_node.parent()},
                ):
                    break
            else:
                yield from self._iter_port_configs(workspace_node, condition_data)

    def _iter_workspace_configs(self, stage_node, stage_config):
        for workspace_name, workspace_data in stage_config["workspaces"].items():
            yield workspace_name, workspace_data

        for condition_data in stage_config.get("conditional", []):
            for conditional in condition_data["conditions"]:
                if not self._resolve_conditional(conditional, {"stage": stage_node}):
                    break
            else:
                yield from self._iter_workspace_configs(stage_node, condition_data)

    # Creates an entity node and all it's contained workspaces. Does not create
    # connections.
    def create_stage_node(self, type, name, data, parent=None):
        config = self._config["stages"][type]
        metadata = self._merge_metadata(config.get("data", {}), data)

        stage_node = Node(type, name, parent=parent, metadata=metadata)
        for workspace_name, workspace_config in self._iter_workspace_configs(
            stage_node, config
        ):
            self._load_workspace(stage_node, workspace_name, workspace_config)

        return stage_node

    def _iter_source_nodes(self, port, connection_data):
        connection_type = connection_data.get("type")
        if connection_type == "internal":
            yield port.node().parent()
        elif connection_type == "foreach":
            foreach_data = connection_data["foreach"]
            item_expression = foreach_data.get("item", "item")
            keywords = {
                "port": port,
                "workspace": port.node(),
                "stage": port.node().parent(),
            }
            for item in exp_parser.parse(foreach_data["loop"], keywords):
                keywords["item"] = item
                conditions = foreach_data.get("conditions", [])
                if all(
                    self._resolve_conditional(conditional, keywords)
                    for conditional in conditions
                ):
                    yield exp_parser.parse(item_expression, keywords)
        else:
            raise ValueError("Unknown connection type: {}".format(connection_type))

    def _load_connections(self, node, workspace_name, workspace_data):
        workspace = node.child(workspace_name)
        for port_type, input_name, input_config in self._iter_port_configs(
            workspace, workspace_data
        ):
            if port_type != constants.PortType.Input:
                continue

            for connection_data in input_config.get("connections", []):
                ws_name = connection_data["workspace"]
                port_type = connection_data["port_type"]
                port_name = connection_data["port_name"]
                metadata = connection_data.get("data", {})

                target_port = workspace.port(constants.PortType.Input, input_name)

                for source_node in self._iter_source_nodes(
                    target_port, connection_data
                ):
                    source_port = source_node.child(ws_name).port(port_type, port_name)
                    # Add a copy of the metadata to each connection so that it's
                    # not shared
                    yield Connection(
                        source_port, target_port, metadata=copy.deepcopy(metadata)
                    )

    # Creates all the connections the configuration defines for the workspaces
    # inside the node. Cannot be given a workspace node directly.
    def create_connections(self, stage_node):
        config = self._config["stages"][stage_node.type()]
        connections = []
        for workspace_name, workspace_config in self._iter_workspace_configs(
            stage_node, config
        ):
            connections.extend(
                self._load_connections(stage_node, workspace_name, workspace_config)
            )

        return connections


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

    with open("/home/mshaw/git/python/pipegraph/config/graph.yml") as f:
        config = yaml.safe_load(f)

    loader = ConfigLoader(config)

    root = Node("root", "pipeline")
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

    def print_tree(n, level=0):
        print(". " * level + n.name(), [p for p in n.ports()])
        for c in n.children():
            print_tree(c, level=level + 1)

    print_tree(root)

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
