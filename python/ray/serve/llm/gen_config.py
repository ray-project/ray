"""Stub for the removed Serve LLM config generator."""
import sys


def main():
    print(
        "The Serve LLM config generator is no longer supported and this command "
        "will be removed in a future Ray version. "
        "See https://recipes.vllm.ai/ for current guidance on serving LLMs.",
        file=sys.stderr,
    )
    raise SystemExit(1)


if __name__ == "__main__":
    main()
