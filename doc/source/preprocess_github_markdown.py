import re
import argparse
import pathlib


def preprocess_github_markdown_file(path: str):
    """
    Preprocesses GitHub Markdown files by:
        - Uncommenting all ``<!-- -->`` comments in which opening tag is immediately
          succeded by ``$UNCOMMENT``(eg. ``<!--$UNCOMMENTthis will be uncommented-->``)
        - Removing text between ``<!--$REMOVE-->`` and ``<!--$END_REMOVE-->``

    This is to enable translation between GitHub Markdown and MyST Markdown used
    in docs. For more details, see ``doc/README.md``.
    """
    with open(path, "r") as f:
        text = f.read()
    # $UNCOMMENT
    text = re.sub(r"<!--\s*\$UNCOMMENT(.*?)(-->)", r"\1", text, flags=re.DOTALL)
    # $REMOVE
    text = re.sub(
        r"(<!--\s*\$REMOVE\s*-->)(.*?)(<!--\s*\$END_REMOVE\s*-->)",
        r"",
        text,
        flags=re.DOTALL,
    )
    with open(path, "w") as f:
        f.write(text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Preprocess github markdown file to Ray Docs MyST markdown"
    )
    parser.add_argument(
        "path", type=pathlib.Path, help="Path to github markdown file to preprocess"
    )
    args, _ = parser.parse_known_args()

    preprocess_github_markdown_file(args.path.expanduser())
