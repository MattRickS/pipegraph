import re


def parse(expression, keywords):
    current = None
    index = 0
    while index < len(expression):
        # Attempts to match any of the following:
        #   word
        #   .word
        #   [word]
        match = re.match(r"((\.)|(\[))?(\w+)(?(3)\])", expression[index:])
        if match is None:
            raise ValueError("Malformed expression")

        accessor, _, _, value = match.groups()
        if current is None:
            current = keywords[value]
        elif accessor == ".":
            current = getattr(current, value)
        elif accessor == "[":
            current = current[value]
        elif accessor is None:
            raise ValueError("Missing accessor for keyword: {}".format(value))
        else:
            raise ValueError("Unknown accessor: {}".format(accessor))

        index += match.end()

    return current


if __name__ == "__main__":

    class A(object):
        parent = {"is_rigged": {"value": True}}

    a = "this[instances]"
    keywords = {"this": {"instances": []}}
    result = parse(a, keywords)
    print("RESULT:", result)

    b = "this.parent[is_rigged][value]"
    keywords = {"this": A}
    result = parse(b, keywords)
    print("RESULT:", result)

    c = "this.parent[is_rigged]value"
    keywords = {"this": A}
    result = parse(c, keywords)
    print("RESULT:", result)
