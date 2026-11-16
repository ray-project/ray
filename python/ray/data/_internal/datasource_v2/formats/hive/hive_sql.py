"""Quote Hive column identifiers for table reads."""


def quote_identifier(name: str) -> str:
    """Quote a literal name in Hive's default quoted-identifier mode."""
    if not name or "\0" in name:
        raise ValueError("HiveServer2 column identifier is invalid")
    return "`" + name.replace("`", "``") + "`"
