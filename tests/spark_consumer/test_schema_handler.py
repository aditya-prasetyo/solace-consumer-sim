"""
Tests for SchemaHandler with registry integration
Covers: schema detection, type inference, registry auto-write, normalize_record
"""

import pytest
from unittest.mock import MagicMock, patch, call

from src.spark_consumer.transformations.schema_handler import (
    SchemaHandler, KNOWN_SCHEMAS
)


@pytest.fixture
def handler():
    return SchemaHandler()


@pytest.fixture
def mock_writer():
    writer = MagicMock()
    writer.register_schema_column.return_value = True
    return writer


@pytest.fixture
def handler_with_writer(mock_writer):
    return SchemaHandler(writer=mock_writer)


class TestSchemaDetection:

    def test_known_columns_loaded(self, handler):
        orders_cols = handler.get_known_columns("ORDERS")
        assert "ORDER_ID" in orders_cols
        assert "CUSTOMER_ID" in orders_cols
        assert "STATUS" in orders_cols

    def test_detect_new_columns(self, handler):
        data = {"ORDER_ID": 1, "DISCOUNT_CODE": "ABC123"}
        new = handler.detect_new_columns("ORDERS", data)
        assert "DISCOUNT_CODE" in new
        assert "ORDER_ID" not in new

    def test_detect_no_new_columns(self, handler):
        data = {"ORDER_ID": 1, "STATUS": "PENDING"}
        new = handler.detect_new_columns("ORDERS", data)
        assert new == []

    def test_detect_unknown_table(self, handler):
        data = {"COL_A": 1, "COL_B": "x"}
        new = handler.detect_new_columns("UNKNOWN_TABLE", data)
        assert set(new) == {"COL_A", "COL_B"}


class TestTypeInference:

    def test_infer_int(self, handler):
        assert handler._infer_type(42) == "int"

    def test_infer_long(self, handler):
        assert handler._infer_type(3_000_000_000) == "long"

    def test_infer_double(self, handler):
        assert handler._infer_type(3.14) == "double"

    def test_infer_string(self, handler):
        assert handler._infer_type("hello") == "string"

    def test_infer_boolean(self, handler):
        assert handler._infer_type(True) == "boolean"

    def test_infer_none(self, handler):
        assert handler._infer_type(None) == "string"

    def test_infer_dict(self, handler):
        assert handler._infer_type({"key": "val"}) == "string"

    def test_infer_list(self, handler):
        assert handler._infer_type([1, 2, 3]) == "string"


class TestRegisterColumns:

    def test_register_new_column(self, handler):
        data = {"ORDER_ID": 1, "LOYALTY_POINTS": 500}
        new = handler.register_columns("ORDERS", data)
        assert "LOYALTY_POINTS" in new
        assert "LOYALTY_POINTS" in handler.get_known_columns("ORDERS")

    def test_register_known_column_no_duplicate(self, handler):
        data = {"ORDER_ID": 1}
        new = handler.register_columns("ORDERS", data)
        assert new == []

    def test_register_new_table(self, handler):
        data = {"ID": 1, "VALUE": "test"}
        new = handler.register_columns("NEW_TABLE", data)
        assert set(new) == {"ID", "VALUE"}
        assert handler.get_known_columns("NEW_TABLE") == {"ID", "VALUE"}

    def test_register_idempotent(self, handler):
        data = {"ORDER_ID": 1, "NEW_COL": "val"}
        handler.register_columns("ORDERS", data)
        new2 = handler.register_columns("ORDERS", data)
        assert new2 == []  # Already registered


class TestRegistryAutoWrite:
    """Test that SchemaHandler writes to cdc_schema_registry via writer"""

    def test_new_column_triggers_registry_write(self, handler_with_writer, mock_writer):
        data = {"ORDER_ID": 1, "DISCOUNT_CODE": "ABC"}
        handler_with_writer.register_columns("ORDERS", data)

        mock_writer.register_schema_column.assert_called_once_with(
            "ORDERS", "DISCOUNT_CODE", "VARCHAR2(255)"
        )

    def test_known_column_no_registry_write(self, handler_with_writer, mock_writer):
        data = {"ORDER_ID": 1, "STATUS": "PENDING"}
        handler_with_writer.register_columns("ORDERS", data)

        mock_writer.register_schema_column.assert_not_called()

    def test_multiple_new_columns_multiple_writes(self, handler_with_writer, mock_writer):
        data = {"ORDER_ID": 1, "COL_A": 42, "COL_B": 3.14}
        handler_with_writer.register_columns("ORDERS", data)

        assert mock_writer.register_schema_column.call_count == 2
        calls = mock_writer.register_schema_column.call_args_list
        call_args = {(c[0][1], c[0][2]) for c in calls}
        assert ("COL_A", "NUMBER(10)") in call_args
        assert ("COL_B", "NUMBER(15,4)") in call_args

    def test_no_writer_no_error(self, handler):
        """Handler without writer should still work (log only)"""
        data = {"ORDER_ID": 1, "NEW_COL": "test"}
        new = handler.register_columns("ORDERS", data)
        assert "NEW_COL" in new  # Still detects

    def test_writer_failure_does_not_block(self, handler_with_writer, mock_writer):
        """If writer.register_schema_column fails, registration still succeeds"""
        mock_writer.register_schema_column.side_effect = Exception("DB down")

        data = {"ORDER_ID": 1, "NEW_COL": "test"}
        new = handler_with_writer.register_columns("ORDERS", data)
        assert "NEW_COL" in new  # Column still tracked in-memory

    def test_type_mapping_boolean(self, handler_with_writer, mock_writer):
        data = {"ORDER_ID": 1, "IS_PREMIUM": True}
        handler_with_writer.register_columns("ORDERS", data)

        mock_writer.register_schema_column.assert_called_once_with(
            "ORDERS", "IS_PREMIUM", "NUMBER(1)"
        )

    def test_type_mapping_long(self, handler_with_writer, mock_writer):
        data = {"ORDER_ID": 1, "BIG_NUM": 3_000_000_000}
        handler_with_writer.register_columns("ORDERS", data)

        mock_writer.register_schema_column.assert_called_once_with(
            "ORDERS", "BIG_NUM", "NUMBER(19)"
        )


class TestSetWriter:

    def test_set_writer_after_init(self):
        handler = SchemaHandler()
        assert handler._writer is None

        mock_writer = MagicMock()
        handler.set_writer(mock_writer)
        assert handler._writer is mock_writer

    def test_set_writer_enables_registry(self):
        handler = SchemaHandler()
        mock_writer = MagicMock()
        mock_writer.register_schema_column.return_value = True

        handler.set_writer(mock_writer)
        handler.register_columns("ORDERS", {"ORDER_ID": 1, "NEW_COL": "x"})

        mock_writer.register_schema_column.assert_called_once()


class TestNormalizeRecord:

    def test_missing_columns_set_to_none(self, handler):
        data = {"ORDER_ID": 1}
        normalized = handler.normalize_record("ORDERS", data)
        assert normalized["ORDER_ID"] == 1
        assert normalized["STATUS"] is None
        assert normalized["TOTAL_AMOUNT"] is None

    def test_extra_columns_preserved(self, handler):
        data = {"ORDER_ID": 1, "NEW_FIELD": "val"}
        normalized = handler.normalize_record("ORDERS", data)
        assert normalized["NEW_FIELD"] == "val"

    def test_dict_values_serialized(self, handler):
        data = {"ORDER_ID": 1, "METADATA": {"key": "val"}}
        normalized = handler.normalize_record("ORDERS", data)
        assert normalized["METADATA"] == '{"key": "val"}'


class TestPreparedDynamicRecord:

    def test_separate_known_and_extra(self, handler):
        data = {"ORDER_ID": 1, "STATUS": "PENDING", "DISCOUNT": 0.1}
        result = handler.prepare_dynamic_record("ORDERS", data)
        assert result["known"]["ORDER_ID"] == 1
        assert result["known"]["STATUS"] == "PENDING"
        assert result["extra"]["DISCOUNT"] == 0.1
        assert result["has_extra"] is True

    def test_no_extra_fields(self, handler):
        data = {"ORDER_ID": 1, "STATUS": "PENDING"}
        result = handler.prepare_dynamic_record("ORDERS", data)
        assert result["has_extra"] is False
        assert result["extra"] == {}
