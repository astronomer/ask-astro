from langchain.prompts import SystemMessagePromptTemplate


def test_system_prompt_loading():
    """Test if the system prompt is loaded correctly"""
    with open("../../../../api/ask_astro/templates/combine_docs_chat_prompt.txt") as fd:
        expected_template = fd.read()
    template_instance = SystemMessagePromptTemplate.from_template(expected_template)
    assert template_instance.prompt.template == expected_template
