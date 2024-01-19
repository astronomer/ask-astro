from typing import Optional

from langchain.output_parsers.pydantic import PydanticOutputParser
from langchain.retrievers.multi_query import LineList


class CustomLineListOutputParser(PydanticOutputParser):
    """
    Output parser for a list of lines with additional cleaning and fail checks.
    This is a modified, less error prone implementation of the LineListOutputParser from LangChain.
    """

    max_lines: int = 2
    max_line_len: Optional[int] = None

    def __init__(self, max_lines: int = 2, max_line_len: Optional[int] = None) -> None:
        """
        Initialize CustomLineListOutputParser.

        :param max_lines: maximum number of lines, default is 2
        :param max_line_len: maximum length of lines, default is None
        """
        super().__init__(pydantic_object=LineList)
        self.max_lines = max_lines
        self.max_line_len = max_line_len

    def parse(self, text: str) -> LineList:
        """
        Parse the input text into LineList.

        :param text: input text to parse
        :return: parsed LineList
        """
        lines = text.strip().split("\n")
        lines = [s for s in lines if s != "" and (self.max_line_len is None or len(s) <= self.max_line_len)][
            : self.max_lines
        ]
        return LineList(lines=lines)
