# TODO: Write a class to handle schema evolution for merge-into
#  on it's own, Spark-Iceberg does not handle this
# https://github.com/apache/iceberg/issues/5556
from dataclasses import dataclass, field
from itertools import zip_longest
from typing import Literal

from pyspark.sql.types import StructField


@dataclass(frozen=True)
class FieldChange:
    field: StructField
    operation: Literal["add", "drop"]


@dataclass(frozen=True)
class AddField(FieldChange):
    operation: Literal["add"] = "add"


@dataclass(frozen=True)
class DropField(FieldChange):
    operation: Literal["drop"] = "drop"


@dataclass
class SchemaPlan:
    fields_to_add: list[AddField] = field(default_factory=list)
    fields_to_drop: list[DropField] = field(default_factory=list)


class SchemaEvolutionHandler:
    def __init__(self, old, new):
        self.old = old
        self.new = new

    def get_diff(self):
        diff = SchemaPlan()
        old_fields_set = set(self.old.fields)
        new_fields_set = set(self.new.fields)
        for i, (old, new) in enumerate(zip_longest(self.old.fields, self.new.fields)):
            match (old, new):
                case (None, col):
                    if col not in old_fields_set:
                        diff.fields_to_add.append(AddField(field=col))
                case (col, None):
                    if col not in new_fields_set:
                        diff.fields_to_drop.append(DropField(field=col))
                case (old, new) if old != new:
                    if new not in old_fields_set:
                        diff.fields_to_add.append(AddField(field=new))
                    if old not in new_fields_set:
                        diff.fields_to_drop.append(DropField(field=old))
                    # there is probably something to do here regarding column ordering
        return diff

    def plan(self):
        pass

    def apply(self):
        pass

    # needs to
    # - potentially handle any nested struct changes
    # out of scope?:
    # - reordering columns (allowable in iceberg but not in snowflake)
