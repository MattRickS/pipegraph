class ConfigurationError(Exception):
    pass


class ExpressionError(Exception):
    pass


class InvalidExpression(ExpressionError):
    pass


class MissingData(ExpressionError):
    pass
