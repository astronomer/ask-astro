import json
import re
import jinja2


def markdown_to_slack(message: str):
    regexp_replacements = (
        (re.compile("^- ", flags=re.M), "• "),
        (re.compile("^  - ", flags=re.M), "  ◦ "),
        (re.compile("^    - ", flags=re.M), "    ⬩ "),  # ◆
        (re.compile("^      - ", flags=re.M), "    ◽ "),
        (re.compile("^#+ (.+)$", flags=re.M), r"*\1*"),
        (re.compile("\*\*"), "*"),
        (re.compile(r"\[(.+)]\((.+)\)"), r"<\2|\1>"),
        (re.compile("```\\S+\\n"), r"```\n"),
    )

    for regex, replacement in regexp_replacements:
        message = regex.sub(replacement, message)

    return message


def get_blocks(block: str, **kwargs):
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader("../templates"), autoescape=True
    )
    return json.loads(env.get_template(block).render(kwargs))["blocks"]
