import copy

import constants
import expression as exp_parser
import nodes


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
        port = nodes.Port(
            port_type,
            port_name,
            multi=port_config.get("multi", False),
            promoted=port_config.get("promote", False),
            metadata=port_config.get("data", {}),
        )
        node.add_port(port)
        return port

    def _load_workspace(self, workspace_node, workspace_name, workspace_data):
        workspace = nodes.Node(
            "workspace",
            workspace_name,
            parent=workspace_node,
            metadata=workspace_data.get("data", {}),
        )
        for port_type, port_name, port_config in self._iter_port_configs(
            workspace, workspace_data
        ):
            self._load_port(workspace, port_type, port_name, port_config)

        return workspace

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

        stage_node = nodes.Node(type, name, parent=parent, metadata=metadata)
        for workspace_name, workspace_config in self._iter_workspace_configs(
            stage_node, config
        ):
            workspace = self._load_workspace(
                stage_node, workspace_name, workspace_config
            )
            for port in workspace.ports():
                if not port.is_promoted():
                    continue

                existing = stage_node.port(port.type(), port.name())
                if existing:
                    existing.share(port)
                else:
                    promoted_port = nodes.PromotedPort(
                        port.type(),
                        port.name(),
                        multi=port.is_multi(),
                        metadata=copy.deepcopy(port.metadata),
                    )
                    promoted_port.share(port)
                    stage_node.add_port(promoted_port)

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
                port_type = connection_data.get("port_type", constants.PortType.Output)
                port_name = connection_data["port_name"]
                metadata = connection_data.get("data", {})

                target_port = workspace.port(constants.PortType.Input, input_name)

                for source_node in self._iter_source_nodes(
                    target_port, connection_data
                ):
                    source_port = source_node.child(ws_name).port(port_type, port_name)
                    # Add a copy of the metadata to each connection so that it's
                    # not shared
                    yield nodes.Connection(
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

    def metadata(self, stage_type):
        metadata = self._config["stages"][stage_type]["data"]
        return copy.deepcopy(metadata or {})


if __name__ == "__main__":
    import yaml

    # TODO: What if ports had a flag to promote themselves as a stage port.
    # Promotions with the same name are merged together, or an optional keyword
    # could be used to separate them, eg, promote_name: "surfModel"
    # promote keyword is an implicit connection from the port to the parent port
    # connection types are stage (external/promoted) and workspace (internal)
    # How would this work with nested stages, eg, sequence/shot? Connection types
    # need to be more explicit.
    #   asset
    #     workspace:
    #       modeling:
    #         output:
    #           animGeo: {promote: True}
    #           blendshape: {}
    #           model: {promote: True}
    #       surfacing:
    #         output:
    #           model: {promote: True}
    #   shot:
    #     workspace:
    #       layout:
    #         input:
    #           model:
    #             promote: True
    #             connections:
    #             - type: stage
    #               port_type: promoted (input? output?)
    #               port_name: model
    # Shared promotion is tricky - both would need to define the same data, eg,
    # same datatype / number of connections / etc...
    # Could enforce it strictly, but how? Why force users to configure the same
    # data twice if it's shared?
    # Parent-child structures are already problematic. Every node must be a
    # separate entry in the graph, the parent-child relationship is only for
    # API navigation and not for visual representation.

    # TODO: Either have a metadata object (with getitem/setitem accessors) that
    # is used for subdicts, or abandon the "type" key and use isinstance in the UI

    with open("/home/mshaw/git/python/pipegraph/config/graph.yml") as f:
        config = yaml.safe_load(f)

    loader = ConfigLoader(config)

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

    def print_tree(n, level=0):
        print(". " * level + n.name(), [p for p in n.ports()])
        for c in n.children():
            print_tree(c, level=level + 1)

    print_tree(root)
