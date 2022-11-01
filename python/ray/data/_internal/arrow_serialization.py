import os

RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION = (
    "RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION"
)


def _register_arrow_json_readoptions_serializer():
    import ray

    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        import logging

        logger = logging.getLogger(__name__)
        logger.info("Disabling custom Arrow JSON ReadOptions serialization.")
        return

    try:
        import pyarrow.json as pajson
    except ModuleNotFoundError:
        return

    ray.util.register_serializer(
        pajson.ReadOptions,
        serializer=lambda opts: (opts.use_threads, opts.block_size),
        deserializer=lambda args: pajson.ReadOptions(*args),
    )


def _register_arrow_json_parseoptions_serializer():
    import ray

    if (
        os.environ.get(
            RAY_DISABLE_CUSTOM_ARROW_JSON_OPTIONS_SERIALIZATION,
            "0",
        )
        == "1"
    ):
        import logging

        logger = logging.getLogger(__name__)
        logger.info("Disabling custom Arrow JSON ParseOptions serialization.")
        return

    try:
        import pyarrow.json as pajson
    except ModuleNotFoundError:
        return

    ray.util.register_serializer(
        pajson.ParseOptions,
        serializer=lambda opts: (
            opts.explicit_schema,
            opts.newlines_in_values,
            opts.unexpected_field_behavior,
        ),
        deserializer=lambda args: pajson.ParseOptions(*args),
    )
