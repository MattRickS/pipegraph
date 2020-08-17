class Port(object):
    def __init__(self, type, name, multi=False, metadata=None):
        self._type = type
        self._name = name
        self._node = None
        self._is_multi = multi
        self.metadata = metadata or {}

    def __getitem__(self, item):
        return self.metadata[item]["value"]

    def __str__(self):
        return "{s.__class__.__name__}({s._type}, {s._name})".format(s=self)

    def __repr__(self):
        return (
            "{s.__class__.__name__}({s._type!r}, {s._name!r}, multi={s._is_multi}, "
            "metadata={s.metadata})".format(s=self)
        )

    def __eq__(self, other):
        return (
            isinstance(other, Port)
            and self.node() == other.node()
            and self.type() == other.type()
            and self.name() == other.name()
        )

    def __hash__(self):
        return hash((self.node(), self.type(), self.name()))

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
    def __init__(self, source, target, group=None, internal=True, metadata=None):
        self._source = source
        self._target = target
        self._internal = internal
        self.group = group
        self.metadata = metadata or {}

    def __getitem__(self, item):
        return self.metadata[item]["value"]

    def __repr__(self):
        return (
            "{s.__class__.__name__}({s._source!r}, {s._target!r}, group={s.group!r}, "
            "internal={s._internal!r}, metadata={s.metadata!r})".format(s=self)
        )

    def __eq__(self, other):
        return (
            isinstance(other, Connection)
            and self.source() == other.source()
            and self.target() == other.target()
        )

    def __hash__(self):
        return hash((self.source(), self.target()))

    def is_internal(self):
        return self._internal

    def source(self):
        return self._source

    def target(self):
        return self._target

    def connected(self, port):
        if port == self.source():
            return self.target()
        elif port == self.target():
            return self.source()
        else:
            raise ValueError("Invalid port")
