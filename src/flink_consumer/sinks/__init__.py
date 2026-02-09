"""
PyFlink CDC Sinks (Dual: PostgreSQL + REST API)
"""

from .postgres_sink import FlinkPostgresSink
from .rest_sink import FlinkRestApiSink

__all__ = ["FlinkPostgresSink", "FlinkRestApiSink"]
