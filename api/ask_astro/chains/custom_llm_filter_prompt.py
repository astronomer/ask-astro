from langchain_core.output_parsers import BaseOutputParser
from langchain_core.prompts import PromptTemplate

prompt_template = """Given the following question starting with "Question:", and context starting with "Context:" surrounded by >>> symbols, return YES if the context can be used to answer the question and NO if it cannot.

> Question: {question}
> Context:
>>>
{context}
>>>
> Context surrounded by >>> is helpful and can be used to answer the question (YES / NO):"""


class CustomBooleanOutputParser(BaseOutputParser[bool]):
    """Parse the output of an LLM call to a boolean. Default to True if response not formatted correctly."""

    true_val: str = "YES"
    """The string value that should be parsed as True."""
    false_val: str = "NO"
    """The string value that should be parsed as False."""

    def parse(self, text: str) -> bool:
        """Parse the output of an LLM call to a boolean by checking if YES/NO is contained in the output.

        Args:
            text: output of a language model.

        Returns:
            boolean

        """
        cleaned_text = text.strip().upper()
        return self.false_val not in cleaned_text

    @property
    def _type(self) -> str:
        """Snake-case string identifier for an output parser type."""
        return "custom_boolean_output_parser"


custom_llm_chain_filter_prompt_template = PromptTemplate(
    template=prompt_template,
    input_variables=["question", "context"],
    output_parser=CustomBooleanOutputParser(),
)
