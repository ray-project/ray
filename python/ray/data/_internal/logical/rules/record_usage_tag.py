from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag
from ray.data._internal.logical.interfaces import LogicalOperator, LogicalPlan, Rule
from ray.data._internal.logical.operators.all_to_all_operator import (
    Aggregate,
    RandomizeBlocks,
    RandomShuffle,
    Repartition,
    Sort,
)
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.map_operator import (
    Filter,
    FlatMap,
    MapBatches,
    MapRows,
    Write,
)
from ray.data.datasource.binary_datasource import BinaryDatasource
from ray.data.datasource.csv_datasource import CSVDatasource
from ray.data.datasource.datasource import Datasource
from ray.data.datasource.image_datasource import ImageDatasource
from ray.data.datasource.json_datasource import JSONDatasource
from ray.data.datasource.mongo_datasource import MongoDatasource
from ray.data.datasource.numpy_datasource import NumpyDatasource
from ray.data.datasource.parquet_base_datasource import ParquetBaseDatasource
from ray.data.datasource.parquet_datasource import ParquetDatasource
from ray.data.datasource.text_datasource import TextDatasource
from ray.data.datasource.tfrecords_datasource import TFRecordDatasource


class RecordUsageTagRule(Rule):
    """Rule for recording telemetry usage tag for logical operators.

    NOTE: this rule just records tag, and does not change logical plan and operators.
    """

    def apply(self, plan: LogicalPlan) -> LogicalPlan:
        self._record_usage_tag(plan.dag)
        return plan

    def _record_usage_tag(self, op: LogicalOperator):
        """Record usage tag for each logical operator in post-order."""
        for child in op.input_dependencies:
            self._record_usage_tag(child)

        if isinstance(op, Read):
            self._record_data_source_usage_tag(op._datasource, True)
        elif isinstance(op, Write):
            self._record_data_source_usage_tag(op._datasource, False)
        elif isinstance(op, MapBatches):
            record_extra_usage_tag(TagKey.DATA_MAP_BATCHES_OP, "1")
        elif isinstance(op, MapRows):
            record_extra_usage_tag(TagKey.DATA_MAP_OP, "1")
        elif isinstance(op, FlatMap):
            record_extra_usage_tag(TagKey.DATA_FLAT_MAP_OP, "1")
        elif isinstance(op, Filter):
            record_extra_usage_tag(TagKey.DATA_FILTER_OP, "1")
        elif isinstance(op, RandomizeBlocks):
            record_extra_usage_tag(TagKey.DATA_RANDOMIZE_BLOCK_ORDER_OP, "1")
        elif isinstance(op, RandomShuffle):
            record_extra_usage_tag(TagKey.DATA_RANDOM_SHUFFLE_OP, "1")
        elif isinstance(op, Repartition):
            record_extra_usage_tag(TagKey.DATA_REPARTITION_OP, "1")
        elif isinstance(op, Sort):
            record_extra_usage_tag(TagKey.DATA_SORT_OP, "1")
        elif isinstance(op, Aggregate):
            record_extra_usage_tag(TagKey.DATA_AGGREGATE_OP, "1")

    def _record_data_source_usage_tag(self, ds: Datasource, is_read: bool):
        """Record usage tag for data source (read and write operator)."""
        if isinstance(ds, ParquetDatasource):
            if is_read:
                record_extra_usage_tag(TagKey.DATA_READ_PARQUET_OP, "1")
            else:
                record_extra_usage_tag(TagKey.DATA_WRITE_PARQUET_OP, "1")
        elif isinstance(ds, ParquetBaseDatasource):
            assert is_read
            record_extra_usage_tag(TagKey.DATA_READ_PARQUET_BULK_OP, "1")
        elif isinstance(ds, ImageDatasource):
            assert is_read
            record_extra_usage_tag(TagKey.DATA_READ_IMAGES_OP, "1")
        elif isinstance(ds, TextDatasource):
            assert is_read
            record_extra_usage_tag(TagKey.DATA_READ_TEXT_OP, "1")
        elif isinstance(ds, BinaryDatasource):
            assert is_read
            record_extra_usage_tag(TagKey.DATA_READ_BINARY_OP, "1")
        elif isinstance(ds, TFRecordDatasource):
            if is_read:
                record_extra_usage_tag(TagKey.DATA_READ_TFRECORDS_OP, "1")
            else:
                record_extra_usage_tag(TagKey.DATA_WRITE_TFRECORDS_OP, "1")
        elif isinstance(ds, CSVDatasource):
            if is_read:
                record_extra_usage_tag(TagKey.DATA_READ_CSV_OP, "1")
            else:
                record_extra_usage_tag(TagKey.DATA_WRITE_CSV_OP, "1")
        elif isinstance(ds, JSONDatasource):
            if is_read:
                record_extra_usage_tag(TagKey.DATA_READ_JSON_OP, "1")
            else:
                record_extra_usage_tag(TagKey.DATA_WRITE_JSON_OP, "1")
        elif isinstance(ds, NumpyDatasource):
            if is_read:
                record_extra_usage_tag(TagKey.DATA_READ_NUMPY_OP, "1")
            else:
                record_extra_usage_tag(TagKey.DATA_WRITE_NUMPY_OP, "1")
        elif isinstance(ds, MongoDatasource):
            if is_read:
                record_extra_usage_tag(TagKey.DATA_READ_MONGO_OP, "1")
            else:
                record_extra_usage_tag(TagKey.DATA_WRITE_MONGO_OP, "1")
        else:
            if is_read:
                record_extra_usage_tag(TagKey.DATA_READ_CUSTOM_OP, "1")
            else:
                record_extra_usage_tag(TagKey.DATA_WRITE_CUSTOM_OP, "1")
