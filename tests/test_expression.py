import pytest

import exceptions, expression


class Temp(object):
    def __init__(self, value):
        self.attr = value


@pytest.mark.parametrize(
    "expr, keywords, expected",
    [
        ("key", {"key": {"has_value": True}}, {"has_value": True}),
        ("key[has_value]", {"key": {"has_value": True}}, True),
        ("a[b][c]", {"a": {"b": {"c": 1}}}, 1),
        ("key.attr", {"key": Temp(1)}, 1),
        ("key.attr[key]", {"key": Temp({"key": "text"})}, "text"),
    ],
)
def test_valid(expr, keywords, expected):
    assert expression.parse(expr, keywords) == expected


@pytest.mark.parametrize(
    "expr, keywords, exc",
    [
        # Invalid access
        ("key", {"missing": 1}, exceptions.MissingData),
        ("key[value]", {"key": {"missing": 1}}, exceptions.MissingData),
        ("key.value", {"key": Temp(1)}, exceptions.MissingData),
        # Broken syntax
        ("key+value", {"key": 1, "value": 2}, exceptions.InvalidExpression),
        ("key[value", {"key": {"value": 1}}, exceptions.InvalidExpression),
        ("key.attr.", {"key": Temp(1)}, exceptions.InvalidExpression),
        ("key..attr", {"key": Temp(1)}, exceptions.InvalidExpression),
        ("a[b]c", {"a": {"b": {"c": 1}}}, exceptions.InvalidExpression),
    ],
)
def test_exception(expr, keywords, exc):
    with pytest.raises(exc):
        expression.parse(expr, keywords)
