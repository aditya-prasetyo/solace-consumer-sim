"""
Debezium CDC Event Simulator
Generates CDC events in Debezium format for Oracle database simulation
"""

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, Optional, Literal
from enum import Enum


class Operation(str, Enum):
    """CDC Operation types"""
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"
    READ = "r"  # Snapshot read


@dataclass
class SourceInfo:
    """Debezium source metadata"""
    version: str = "2.4.0"
    connector: str = "oracle"
    name: str = "oracle-cdc"
    ts_ms: int = 0
    snapshot: str = "false"
    db: str = "ORCL"
    schema: str = "SALES"
    table: str = ""
    txId: str = ""
    scn: str = ""

    def __post_init__(self):
        if not self.ts_ms:
            self.ts_ms = int(time.time() * 1000)
        if not self.txId:
            self.txId = f"{int(time.time()):012d}"
        if not self.scn:
            self.scn = str(int(time.time() * 1000000))


@dataclass
class DebeziumPayload:
    """Debezium event payload"""
    before: Optional[Dict[str, Any]]
    after: Optional[Dict[str, Any]]
    source: SourceInfo
    op: str
    ts_ms: int = 0

    def __post_init__(self):
        if not self.ts_ms:
            self.ts_ms = int(time.time() * 1000)


@dataclass
class DebeziumEvent:
    """Complete Debezium CDC event"""
    payload: DebeziumPayload

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "payload": {
                "before": self.payload.before,
                "after": self.payload.after,
                "source": asdict(self.payload.source),
                "op": self.payload.op,
                "ts_ms": self.payload.ts_ms
            }
        }

    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())


class DebeziumEventBuilder:
    """Builder for creating Debezium CDC events"""

    def __init__(
        self,
        connector: str = "oracle",
        db_name: str = "ORCL",
        schema_name: str = "SALES",
        version: str = "2.4.0"
    ):
        self.connector = connector
        self.db_name = db_name
        self.schema_name = schema_name
        self.version = version
        self._scn_counter = int(time.time() * 1000000)

    def _next_scn(self) -> str:
        """Generate next SCN (System Change Number)"""
        self._scn_counter += 1
        return str(self._scn_counter)

    def _create_source(self, table: str) -> SourceInfo:
        """Create source metadata"""
        return SourceInfo(
            version=self.version,
            connector=self.connector,
            name=f"{self.connector}-cdc",
            ts_ms=int(time.time() * 1000),
            snapshot="false",
            db=self.db_name,
            schema=self.schema_name,
            table=table,
            txId=f"{int(time.time()):012d}",
            scn=self._next_scn()
        )

    def create_insert(self, table: str, data: Dict[str, Any]) -> DebeziumEvent:
        """Create INSERT event"""
        source = self._create_source(table)
        payload = DebeziumPayload(
            before=None,
            after=data,
            source=source,
            op=Operation.CREATE.value
        )
        return DebeziumEvent(payload=payload)

    def create_update(
        self,
        table: str,
        before: Dict[str, Any],
        after: Dict[str, Any]
    ) -> DebeziumEvent:
        """Create UPDATE event"""
        source = self._create_source(table)
        payload = DebeziumPayload(
            before=before,
            after=after,
            source=source,
            op=Operation.UPDATE.value
        )
        return DebeziumEvent(payload=payload)

    def create_delete(self, table: str, data: Dict[str, Any]) -> DebeziumEvent:
        """Create DELETE event"""
        source = self._create_source(table)
        payload = DebeziumPayload(
            before=data,
            after=None,
            source=source,
            op=Operation.DELETE.value
        )
        return DebeziumEvent(payload=payload)

    def create_event(
        self,
        table: str,
        operation: Operation,
        data: Dict[str, Any],
        before_data: Optional[Dict[str, Any]] = None
    ) -> DebeziumEvent:
        """Create event based on operation type"""
        if operation == Operation.CREATE:
            return self.create_insert(table, data)
        elif operation == Operation.UPDATE:
            return self.create_update(table, before_data or {}, data)
        elif operation == Operation.DELETE:
            return self.create_delete(table, data)
        else:
            raise ValueError(f"Unsupported operation: {operation}")
