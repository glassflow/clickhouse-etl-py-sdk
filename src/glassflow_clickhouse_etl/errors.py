"""Custom exceptions for the Glassflow Kafka ClickHouse SDK."""


class GlassflowError(Exception):
    """Base exception for all Glassflow SDK errors."""

    pass


class PipelineAlreadyExistsError(GlassflowError):
    """Exception raised when a pipeline already exists."""

    pass


class PipelineNotFoundError(GlassflowError):
    """Exception raised when a pipeline is not found."""

    pass


class InvalidPipelineConfigError(GlassflowError):
    """Exception raised when a pipeline configuration is invalid."""

    pass


class ConnectionError(GlassflowError):
    """Exception raised when a connection error occurs."""

    pass


class InternalServerError(GlassflowError):
    """Exception raised when an internal server error occurs."""

    pass


class InvalidDataTypeMappingError(GlassflowError):
    """Exception raised when a data type mapping is invalid."""

    pass
