import pytest
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from more_sqlframe.schema_evolution import SchemaEvolutionHandler


class TestSchemaEvolutionHandler:
    @pytest.fixture
    def existing_df(self, spark_session):
        data = [
            {"f1": 1, "f2": "a", "f3": [1, 2, 3], "f4": True},
            {"f1": 2, "f2": "b", "f3": [4, 5, 6], "f4": False},
        ]
        return spark_session.createDataFrame(data=data)

    class TestGetDiff:
        def test_when_a_new_column_is_added(self, existing_df: DataFrame):
            new_df = existing_df.withColumn("new_col", F.current_timestamp())
            handler = SchemaEvolutionHandler(existing_df.schema, new_df.schema)
            diff = handler.get_diff()
            assert len(diff.fields_to_add) == 1
            assert len(diff.fields_to_drop) == 0
            assert diff.fields_to_add[0].field == T.StructField(
                "new_col", T.TimestampType(), False
            )

        def test_when_a_new_column_is_dropped(self, existing_df: DataFrame):
            new_df = existing_df.drop("f4")
            handler = SchemaEvolutionHandler(existing_df.schema, new_df.schema)
            diff = handler.get_diff()
            assert len(diff.fields_to_add) == 0
            assert len(diff.fields_to_drop) == 1
            assert diff.fields_to_drop[0].field == existing_df.schema.fields[-1]

        def test_when_columns_are_reordered(self, existing_df: DataFrame):
            new_df = existing_df.select(*reversed(existing_df.columns))
            handler = SchemaEvolutionHandler(existing_df.schema, new_df.schema)
            diff = handler.get_diff()
            assert len(diff.fields_to_add) == 0
            assert len(diff.fields_to_drop) == 0

        def test_when_columns_are_reordered_and_there_is_new_column(
            self, existing_df: DataFrame
        ):
            new_df = existing_df.withColumn("new_col", F.current_timestamp())
            new_df = new_df.select(*reversed(new_df.columns))
            handler = SchemaEvolutionHandler(existing_df.schema, new_df.schema)
            diff = handler.get_diff()
            assert len(diff.fields_to_add) == 1
            assert len(diff.fields_to_drop) == 0
            assert diff.fields_to_add[0].field == T.StructField(
                "new_col", T.TimestampType(), False
            )
