# import typer

from ray.llm._internal.serve.config_generator.start import gen_config

# app = typer.Typer(
#     help="Ray Serve LLM CLI tools for configuration and management."
# )

# # The correct way to register a command with Typer
# @app.command(name="gen-config")
# def gen_config_command():
#     """Generate configuration for Ray Serve LLM."""
#     gen_config()

# You can add more commands here as needed
# For example:
# @app.command(name="another-command")
# def another_command(param: str = typer.Option(..., help="Parameter description")):
#     """Command description."""
#     # Command implementation


def main():
    """Entry point for the CLI when called as a module."""
    # app()
    gen_config()


if __name__ == "__main__":
    main()
