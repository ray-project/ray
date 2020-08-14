# Experimental.
# Determines the allowed values for all PG-specific TrainerConfigDict settings.
pg_config_schema = {
    "properties": {
        "num_workers": {"type": "integer", "minimum": 0},
        "lr": {"type": "number", "exclusiveMinimum": 0.0},
    }
}
