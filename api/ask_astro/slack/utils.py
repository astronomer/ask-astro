from __future__ import annotations

import json
import re
from typing import Any

import jinja2


def markdown_to_slack(message: str) -> str:
    """
    Convert markdown formatted text to Slack's message format.

    :param message: A string containing markdown formatted text.
    """

    regexp_replacements = (
        (re.compile("^- ", flags=re.M), "• "),
        (re.compile("^  - ", flags=re.M), "  ◦ "),
        (re.compile("^    - ", flags=re.M), "    ⬩ "),  # ◆
        (re.compile("^      - ", flags=re.M), "    ◽ "),
        (re.compile("^#+ (.+)$", flags=re.M), r"*\1*"),
        (re.compile(r"\*\*"), "*"),
        (re.compile(r"\[(.+)]\((.+)\)"), r"<\2|\1>"),
        (re.compile("```\\S+\\n"), r"```\n"),
    )

    for regex, replacement in regexp_replacements:
        message = regex.sub(replacement, message)

    return message


def get_blocks(block: str, **kwargs: Any) -> list[dict[str, Any]]:
    """
    Retrieve a list of Slack blocks by rendering a Jinja2 template.

    :param block: Name of the Jinja2 template to render.
    :param kwargs: Arguments to be passed to the Jinja2 template.
    """
    env = jinja2.Environment(loader=jinja2.FileSystemLoader("ask_astro/templates"), autoescape=True)
    return json.loads(env.get_template(block).render(kwargs))["blocks"]
