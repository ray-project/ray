from ray.llm._internal.serve.config_generator.start import gen_config


def main():
    """Entry point for the CLI when called as a module."""
    gen_config()


if __name__ == "__main__":
    main()
