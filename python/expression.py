import re

import exceptions


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
