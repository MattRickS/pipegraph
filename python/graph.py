import copy

import expression as exp_parser


class Port(object):
    def __init__(self, type, name, multi=False):
        self._node = None
        self._type = type
        self._name = name
        self._is_multi = multi

    def __repr__(self):
        return (
            "{s.__class__.__name__}({s._type!r}, {s._name!r}, "
            "multi={s._is_multi})".format(s=self)
        )

    def is_multi(self):
        return self._is_multi

    @property
    def node(self):
        return self._node

    def name(self):
        return self._name

    def type(self):
        return self._type


class Node(object):
    def __init__(self, type, name, parent=None):
        self._type = type
        self._name = name
        self._parent = parent
        self._children = []
        self._ports = []

        if parent is not None:
            self.set_parent(parent)

    def __repr__(self):
        return "{s.__class__.__name__}({s._type!r}, {s._name!r})".format(s=self)

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

    @property
    def parent(self):
        return self._parent

    def add_port(self, port):
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
    def __init__(self, source, target):
        self._source = source
        self._target = target

    def source(self):
        return self._source

    def target(self):
        return self._target


class DataNode(Node):
    def __init__(self, type, name, parent=None, metadata=None):
        super().__init__(type, name, parent=parent)
        self._metadata = metadata or {}

    def metadata(self, field):
        return self._metadata[field]

    def __getitem__(self, item):
        return self.metadata(item)["value"]


class Graph(object):
    def __init__(self, name):
        self._root = DataNode("root", name)
        self._connections = []

    def root(self):
        return self._root

    def connect(self, source_port, target_port):
        connection = Connection(source_port, target_port)
        self._connections.append(connection)
        return connection

    def connected(self, port):
        for connection in self._connections:
            if connection.source() == port:
                yield connection.target()
            elif connection.target() == port:
                yield connection.source()

    def node(self, path):
        current = self._root
        for name in path:
            current = current.child(name)
            if current is None:
                break

        return current


class ConfigLoader(object):
    def __init__(self, config):
        self._config = config

    def _merge_metadata(self, metadata, data):
        d = copy.deepcopy(metadata)
        # TODO: Recursive merge
        d.update(data)
        return d

    # TODO: All uses should pass in {stage, workspace, port} where possible
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
        port = Port(port_type, port_name, multi=port_config.get("multi", False))
        node.add_port(port)

    def _load_workspace(self, workspace_node, workspace_name, workspace_data):
        workspace = DataNode(
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
        for input_name, input_data in workspace_config.get("inputs", {}).items():
            yield "input", input_name, input_data

        for output_name, output_data in workspace_config.get("outputs", {}).items():
            yield "output", output_name, output_data

        for condition_data in workspace_config.get("conditional", []):
            for conditional in condition_data["conditions"]:
                if not self._resolve_conditional(
                    conditional,
                    {"workspace": workspace_node, "stage": workspace_node.parent},
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
    def create_stage_node(self, type, name, data, parent):
        config = self._config["nodes"][type]
        metadata = self._merge_metadata(config.get("data", {}), data)

        stage_node = DataNode(type, name, parent=parent, metadata=metadata)
        for workspace_name, workspace_config in self._iter_workspace_configs(
            stage_node, config
        ):
            self._load_workspace(stage_node, workspace_name, workspace_config)

        return stage_node

    def _iter_source_nodes(self, port, connection_data):
        connection_type = connection_data.get("type")
        if connection_type == "internal":
            yield port.node.parent
        elif connection_type == "foreach":
            foreach_data = connection_data["foreach"]
            keywords = {
                "port": port,
                "workspace": port.node,
                "stage": port.node.parent,
            }
            for item in exp_parser.parse(foreach_data["loop"], keywords):
                keywords["item"] = item
                conditions = foreach_data.get("conditions", [])
                if all(
                    self._resolve_conditional(conditional, keywords)
                    for conditional in conditions
                ):
                    yield exp_parser.parse(foreach_data["item"], keywords)
        else:
            raise ValueError("Unknown connection type: {}".format(connection_type))

    def _load_connections(self, node, workspace_name, workspace_data):
        workspace = node.child(workspace_name)
        for port_type, input_name, input_config in self._iter_port_configs(
            workspace, workspace_data
        ):
            if port_type != "input":
                continue

            for connection_data in input_config.get("connections", []):
                ws_name = connection_data["workspace"]
                port_type = connection_data["port_type"]
                port_name = connection_data["port_name"]

                target_port = workspace.port("input", input_name)

                for source_node in self._iter_source_nodes(
                    target_port, connection_data
                ):
                    source_port = source_node.child(ws_name).port(port_type, port_name)
                    # TODO: Validate "multi" status of ports against number of connections
                    yield Connection(source_port, target_port)

    # Creates all the connections the configuration defines for the workspaces
    # inside the node. Cannot be given a workspace node directly.
    def create_connections(self, stage_node):
        config = self._config["nodes"][stage_node.type()]
        connections = []
        for workspace_name, workspace_config in self._iter_workspace_configs(
            stage_node, config
        ):
            connections.extend(
                self._load_connections(stage_node, workspace_name, workspace_config)
            )

        return connections


if __name__ == "__main__":
    import yaml

    with open("/home/mshaw/git/python/pipegraph/config/graph.yml") as f:
        config = yaml.safe_load(f)

    loader = ConfigLoader(config)

    class Instance(object):
        def __init__(self, asset):
            self.asset = asset

    graph = Graph("pipeline")
    project = loader.create_stage_node("project", "project", {}, graph.root())
    assetA = loader.create_stage_node("asset", "assetA", {}, project)
    assetB = loader.create_stage_node(
        "asset", "assetB", {"is_rigged": {"type": bool, "value": True}}, project
    )
    shot = loader.create_stage_node(
        "shot",
        "shotA",
        {"instances": {"type": "list", "value": [Instance(assetA), Instance(assetB)]}},
        project,
    )

    def print_tree(n, level=0):
        print(". " * level + n.name(), [p for p in n.ports()])
        for c in n.children():
            print_tree(c, level=level + 1)

    print_tree(graph.root())

    def path(port):
        p = [port.name()]
        n = port.node
        while n:
            p.append(n.name())
            n = n.parent
        return ".".join(p[::-1])

    def print_connection(connection):
        print("{} -> {}".format(path(connection.source()), path(connection.target())))

    # project_c = loader.create_connections(project)
    # assetA_c = loader.create_connections(assetA)
    # assetB_c = loader.create_connections(assetB)
    shot_c = loader.create_connections(shot)
    for connection in shot_c:
        print_connection(connection)
