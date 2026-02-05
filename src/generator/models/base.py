"""
Base Model for Data Generation
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import random


@dataclass
class GeneratedRecord:
    """Container for generated record with metadata"""
    table_name: str
    data: Dict[str, Any]
    operation: str  # 'insert', 'update', 'delete'
    before_data: Optional[Dict[str, Any]] = None


class BaseTableGenerator(ABC):
    """Abstract base class for table data generators"""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self._id_counter = 0
        self._existing_ids: List[int] = []
        self._records: Dict[int, Dict[str, Any]] = {}

    @abstractmethod
    def generate_insert(self) -> Dict[str, Any]:
        """Generate data for INSERT operation"""
        pass

    def generate_update(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Generate data for UPDATE operation (before, after)"""
        if not self._existing_ids:
            # No existing records, generate insert instead
            data = self.generate_insert()
            return {}, data

        record_id = random.choice(self._existing_ids)
        before = self._records.get(record_id, {}).copy()
        after = self._modify_record(before.copy())
        self._records[record_id] = after
        return before, after

    def generate_delete(self) -> Dict[str, Any]:
        """Generate data for DELETE operation"""
        if not self._existing_ids:
            # No existing records, return empty
            return self.generate_insert()

        record_id = random.choice(self._existing_ids)
        data = self._records.pop(record_id, {})
        self._existing_ids.remove(record_id)
        return data

    @abstractmethod
    def _modify_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Modify a record for UPDATE operation"""
        pass

    def _next_id(self) -> int:
        """Generate next ID"""
        self._id_counter += 1
        return self._id_counter

    def _store_record(self, record_id: int, record: Dict[str, Any]):
        """Store record for future updates/deletes"""
        self._existing_ids.append(record_id)
        self._records[record_id] = record
        # Keep only last 10000 records to prevent memory issues
        if len(self._existing_ids) > 10000:
            old_id = self._existing_ids.pop(0)
            self._records.pop(old_id, None)

    def generate(self, operation: str) -> GeneratedRecord:
        """Generate record based on operation"""
        if operation == "insert":
            data = self.generate_insert()
            return GeneratedRecord(
                table_name=self.table_name,
                data=data,
                operation=operation
            )
        elif operation == "update":
            before, after = self.generate_update()
            return GeneratedRecord(
                table_name=self.table_name,
                data=after,
                operation=operation,
                before_data=before
            )
        elif operation == "delete":
            data = self.generate_delete()
            return GeneratedRecord(
                table_name=self.table_name,
                data=data,
                operation=operation,
                before_data=data
            )
        else:
            raise ValueError(f"Unknown operation: {operation}")
