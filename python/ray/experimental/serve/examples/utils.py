import json

from pygments import formatters, highlight, lexers


def pprint_color_json(d):
    formatted_json = json.dumps(d, sort_keys=True, indent=4)

    colorful_json = highlight(formatted_json, lexers.JsonLexer(),
                              formatters.TerminalFormatter())
    print(colorful_json)
