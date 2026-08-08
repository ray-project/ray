from typing import TYPE_CHECKING, Iterator, List, Optional

from ray.data.block import Block
from ray.data.datasource.file_based_datasource import FileBasedDatasource

if TYPE_CHECKING:
    import pyarrow
    import pyarrow.fs
    import pyarrow.lib
    import pyarrow.orc


class ORCDatasource(FileBasedDatasource):
    """A datasource that reads ORC files."""

    _FILE_EXTENSIONS = ["orc"]

    def supports_projection_pushdown(self) -> bool:
        # Use the Ray Data V1 projection optimizer: physical columns are
        # pruned at stripe-read time. Synthetic partition/path columns and
        # output ordering are finalized by ``FileBasedDatasource.read_files``
        # after each block is emitted.
        return True

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import pyarrow as pa
        from pyarrow import orc

        orc_file = orc.ORCFile(f)

        # Translate the current projection into a list of physical ORC column
        # names that will be passed to ``ORCFile.read_stripe``. Synthetic
        # names (partition keys, ``path``) must NEVER be passed to PyArrow ORC
        # or it raises ``Invalid column selected``. When projection pushdown
        # is inactive (``_projection_map is None``) we read all physical
        # columns and let the legacy Project above the read prune them.
        physical_columns = self._select_physical_orc_columns(orc_file)

        if physical_columns is not None and len(physical_columns) == 0:
            # PyArrow ORC gives back a zero-row batch for ``read_stripe`` with
            # ``columns=[]`` even on a nonempty stripe, which would silently
            # break ``count()``/``take_all()``. Read one cheap physical
            # column instead to preserve the stripe's row count. The base
            # class strips this carrier column during the final projection
            # step.
            if not orc_file.schema.names:
                raise ValueError(
                    "ORC file has no physical columns; projection pushdown "
                    "cannot preserve row count without at least one carrier."
                )
            physical_columns = [self._pick_carrier_column(orc_file.schema)]

        # Read one stripe at a time rather than the whole file to bound per-task
        # memory usage on large files. Output block shaping (coalescing small
        # stripes and splitting large ones to the target block size) is handled
        # by the read operator's BlockMapTransformFn, so no manual buffering is
        # needed here.
        for stripe_index in range(orc_file.nstripes):
            if physical_columns is None:
                table = orc_file.read_stripe(stripe_index)
            else:
                table = orc_file.read_stripe(stripe_index, columns=physical_columns)
            yield pa.Table.from_batches([table])

    def _open_input_source(
        self,
        filesystem: "pyarrow.fs.FileSystem",
        path: str,
        **open_args,
    ) -> "pyarrow.NativeFile":
        # ORC stores its metadata footer at the end of the file, so reading
        # requires a seekable file (open_input_file) rather than a sequential
        # input stream.
        return filesystem.open_input_file(path)

    def _select_physical_orc_columns(
        self, orc_file: "pyarrow.orc.ORCFile"
    ) -> Optional[List[str]]:
        """Return the physical ORC column names to read for the current
        projection, or ``None`` to read all physical columns.

        Filters out synthetic names (partition keys and ``path``) which are
        not present in the ORC schema and would cause PyArrow ORC to raise.
        """
        if self._projection_map is None:
            return None
        file_physical = list(orc_file.schema.names)
        physical_set = set(file_physical)
        # Intersection of requested names with the ORC file's physical schema.
        # Preserve the projection's requested order so the base class's final
        # ``BlockAccessor.select`` only has to reorder, not resort.
        return [
            name
            for name in self._projection_map
            if name in physical_set and name in file_physical
        ]

    @staticmethod
    def _pick_carrier_column(orc_schema: "pyarrow.lib.Schema") -> str:
        """Choose the cheapest physical ORC column to read as a row-count
        carrier when no projection columns are physical.

        Prefers narrow fixed-width types (int / bool) over large string /
        binary / struct payloads to minimize I/O on big ORC files. Falls
        back to the first declared field when nothing looks obviously
        cheap, so the choice stays deterministic.
        """
        cheap_kinds = {
            "int",
            "bool",
            "date",
            "timestamp",
            "time",
            "float",
        }
        # First pass: walk in declared order and pick the first "cheap" type.
        for field in orc_schema:
            if str(field.type).split("[", 1)[0].split("(", 1)[0] in cheap_kinds:
                return field.name
        # Fallback: first declared field.
        return orc_schema.field(0).name
