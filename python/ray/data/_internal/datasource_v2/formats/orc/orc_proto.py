"""Private, wire-compatible subset of ORC's proto2 metadata schema.

Field numbers/types come from Apache ORC format, revision
44ca5115b4f3690e1f3d9a3a1100f7b4b1a61fea, src/main/proto/orc/proto/orc_proto.proto
(Apache-2.0). Unused fields remain unknown fields. In particular, proto2 presence
must not be replaced with default scalar values when interpreting statistics.
This is metadata only; no streams, row indexes, or data encodings are decoded.
"""

from typing import Any, Callable, Protocol, Type, cast

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.descriptor import Descriptor
from google.protobuf.message import Message


class _LegacyMessageFactory(Protocol):
    def GetPrototype(self, descriptor: Descriptor) -> Type[Message]:
        ...


def _message_types():
    descriptor = descriptor_pb2.FileDescriptorProto(
        name="ray_orc_metadata.proto", package="ray.orc.metadata", syntax="proto2"
    )
    # name, field number, scalar/message type, repeated. Enum values are read as
    # uint32 so an unknown format/type/codec can be rejected without losing it.
    definitions = {
        "IntegerStatistics": [("minimum", 1, "sint64"), ("maximum", 2, "sint64")],
        "StringStatistics": [
            ("minimum", 1, "string"),
            ("maximum", 2, "string"),
            ("lower_bound", 4, "string"),
            ("upper_bound", 5, "string"),
        ],
        "BucketStatistics": [("count", 1, "uint64", True)],
        "ColumnStatistics": [
            ("number_of_values", 1, "uint64"),
            ("int_statistics", 2, "IntegerStatistics"),
            ("string_statistics", 4, "StringStatistics"),
            ("bucket_statistics", 5, "BucketStatistics"),
            ("has_null", 10, "bool"),
        ],
        "Type": [
            ("kind", 1, "uint32"),
            ("subtypes", 2, "uint32", True),
            ("field_names", 3, "string", True),
        ],
        "StripeInformation": [
            ("offset", 1, "uint64"),
            ("index_length", 2, "uint64"),
            ("data_length", 3, "uint64"),
            ("footer_length", 4, "uint64"),
            ("number_of_rows", 5, "uint64"),
        ],
        "StripeStatistics": [("col_stats", 1, "ColumnStatistics", True)],
        "Metadata": [("stripe_stats", 1, "StripeStatistics", True)],
        "Footer": [
            ("stripes", 3, "StripeInformation", True),
            ("types", 4, "Type", True),
            ("number_of_rows", 6, "uint64"),
            ("statistics", 7, "ColumnStatistics", True),
            ("writer", 9, "uint32"),
            ("encryption", 10, "bytes"),
            ("software_version", 12, "string"),
        ],
        "PostScript": [
            ("footer_length", 1, "uint64"),
            ("compression", 2, "uint32"),
            ("compression_block_size", 3, "uint64"),
            ("version", 4, "uint32", True),
            ("metadata_length", 5, "uint64"),
            ("writer_version", 6, "uint32"),
            ("stripe_statistics_length", 7, "uint64"),
            ("magic", 8000, "string"),
        ],
    }
    field_type = descriptor_pb2.FieldDescriptorProto
    for name, fields in definitions.items():
        message = descriptor.message_type.add(name=name)
        for spec in fields:
            field_name, number, kind, *repeated = spec
            field = message.field.add(
                name=field_name,
                number=number,
                label=field_type.LABEL_REPEATED
                if repeated
                else field_type.LABEL_OPTIONAL,
            )
            if kind in definitions:
                field.type = field_type.TYPE_MESSAGE
                field.type_name = ".ray.orc.metadata." + kind
            else:
                field.type = getattr(field_type, "TYPE_" + kind.upper())
                if repeated and kind != "string":
                    field.options.packed = True
    pool = descriptor_pool.DescriptorPool()
    pool.Add(descriptor)
    result = {}
    for name in definitions:
        desc = pool.FindMessageTypeByName("ray.orc.metadata." + name)
        # Ray supports protobuf 3.20 as well as releases which removed
        # MessageFactory.GetPrototype.
        if hasattr(message_factory, "GetMessageClass"):
            result[name] = message_factory.GetMessageClass(desc)
        else:
            factory = cast(_LegacyMessageFactory, message_factory.MessageFactory(pool))
            result[name] = factory.GetPrototype(desc)
    return result


_TYPES = _message_types()
# Descriptor-defined fields are absent from protobuf's static Message base type.
PostScript: Callable[..., Any] = _TYPES["PostScript"]
Footer: Callable[..., Any] = _TYPES["Footer"]
Metadata: Callable[..., Any] = _TYPES["Metadata"]
