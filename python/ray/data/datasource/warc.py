from collections.abc import Iterator
from ray.data.block import Block
from ray.data.datasource import FileBasedDatasource


# Copied from huggingface/datatrove (Apache-2.0 license)
# https://github.com/huggingface/datatrove/blob/202f99ed8bc66b671226f6a3bbc4bfae0c077f07/src/datatrove/pipeline/readers/warc.py#L84-L131
def process_record(record: "ArcWarcRecord") -> dict | None:
    """Process a WARC record to extract the html and metadata (id, url, date)."""
    import cchardet
    import magic

    # record type
    if record.rec_type != "response" and record.rec_type != "conversion":  # wet files have "conversion" type
        return

    # content type filtering
    mime_type = record.rec_headers.get("WARC-Identified-Payload-Type", None)
    if mime_type is not None and (
        mime_type != "text/html" and (record.rec_type != "conversion" or mime_type != "text/plain")
    ):
        return

    content_bytes = record.content_stream().read()
    if mime_type is None:
        # fallback for older crawls without payload types
        mime_type = magic.from_buffer(content_bytes, mime=True)
        if mime_type != "text/html" and (record.rec_type != "conversion" or mime_type != "text/plain"):
            return

    # Decode the response bytes
    charset = "UTF-8"
    try:
        html = content_bytes.decode(charset)
    except UnicodeDecodeError:
        encoding_det = cchardet.detect(content_bytes)["encoding"]
        if not encoding_det or encoding_det == charset:
            return
        charset = encoding_det

        try:
            html = content_bytes.decode(charset)
        except (UnicodeDecodeError, LookupError):
            return

    id_ = record.rec_headers["WARC-Record-ID"]
    url = record.rec_headers.get("WARC-Target-URI", None)
    date = record.rec_headers.get("WARC-Date", None)
    # handle older formats
    if not url:
        url = dict(record.rec_headers.headers)["uri"]
    if not date:
        date = dict(record.rec_headers.headers)["archive-date"]

    return {"text": html, "id": id_, "url": url, "date": date}


class WarcDatasource(FileBasedDatasource):
    def __init__(self, paths: str | list[str], *, block_size: int | None = None):
        super().__init__(
            paths,
            file_extensions=["warc", "arc"],
        )
        self.block_size = block_size

    def _read_stream(self, f: "pyarrow.NativeFile", path: str) -> Iterator[Block]:
        import uuid
        from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
        from warcio.archiveiterator import ArchiveIterator

        builder = DelegatingBlockBuilder()
        loaded_records = 0
        for record in ArchiveIterator(f):
            extracted_data = process_record(record)
            if extracted_data is not None:
                builder.add(extracted_data)
                loaded_records += 1

            if self.block_size is not None and loaded_records % self.block_size == 0:
                yield builder.build()
                builder = DelegatingBlockBuilder()

        yield builder.build()
