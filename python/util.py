import enum
import re

import exceptions


class Subtype(enum.Enum):
    Boolean = "bool"
    Dict = "dict"
    Integer = "int"
    Float = "float"
    List = "list"
    Mixed = "mixed"
    String = "str"


def collapse_meta(meta):
    value = meta["value"]
    if meta["type"] == "dict" and meta["subtype"] == Subtype.Mixed.value:
        value = collapse_metadata_dict(value)
    elif meta["type"] == "list" and meta["subtype"] == Subtype.Mixed.value:
        value = collapse_metadata_list(value)
    return value


def collapse_metadata_list(metadata):
    return [collapse_meta(meta) for meta in metadata]


def collapse_metadata_dict(metadata):
    return {key: collapse_meta(meta) for key, meta in metadata.items()}


def parse_expression(expression, keywords):
    current = None
    index = 0
    while index < len(expression):
        # Attempts to match any of the following:
        #   word
        #   .word
        #   [word]
        match = re.match(r"((\.)|(\[))?(\w+)(?(3)\])", expression[index:])
        if match is None:
            raise exceptions.InvalidExpression("Malformed expression")

        accessor, _, _, value = match.groups()
        try:
            if current is None:
                current = keywords[value]
            elif accessor == ".":
                current = getattr(current, value)
            elif accessor == "[":
                current = current[value]
            elif accessor is None:
                raise exceptions.InvalidExpression(
                    "Missing accessor for keyword: {}".format(value)
                )
            else:
                raise exceptions.InvalidExpression(
                    "Unknown accessor: {}".format(accessor)
                )
        except (KeyError, TypeError, AttributeError) as e:
            raise exceptions.MissingData(
                "Failed to resolve expression {} with keywords {}: {}".format(
                    expression, keywords, e
                )
            ) from e

        index += match.end()

    return current


if __name__ == "__main__":
    m = {
        "one": {
            "type": "dict",
            "value": {
                "two": {"type": "int", "value": 1},
                "three": {"type": "str", "value": "word"},
            },
        },
        "four": {
            "type": "list",
            "value": [
                {"type": "bool", "value": True},
                {"type": "dict", "value": {"key": {"type": "float", "value": 1.0}}},
            ],
        },
    }
    print(collapse_metadata_dict(m))
