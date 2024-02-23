from __future__ import annotations

import pandas as pd

from include.tasks.extract.utils.stack_overflow_helpers import (
    combine_stack_dfs,
    fetch_questions_through_stack_api,
    process_stack_api_answers,
    process_stack_api_posts,
    process_stack_api_questions,
)


def extract_stack_overflow(
    tag: str, stackoverflow_cutoff_date: str, *, page_size: int = 100, max_pages: int = 10000000
) -> pd.DataFrame:
    """
    This task generates stack overflow documents as a single markdown document per question with associated comments
    and answers. The task returns a pandas dataframe with all documents.

    param tag: The tag names to include in extracting from stack overflow.
    This is used for populating the 'docSource'
    type tag: str

    param stackoverflow_cutoff_date: Only messages from after this date will be extracted.
    type stackoverflow_cutoff_date: str

    returned dataframe fields are:
    'docSource': 'stackoverflow' plus the tag name (ie. 'airflow')
    'docLink': URL for the base question.
    'content': The question (plus answers) in markdown format.
    'sha': a UUID based on the other fields.  This is for compatibility with other document types.
    """

    questions = fetch_questions_through_stack_api(
        tag=tag,
        stackoverflow_cutoff_date=stackoverflow_cutoff_date,
        page_size=page_size,
        max_pages=max_pages,
    )
    posts_df = process_stack_api_posts(questions)
    questions_df = process_stack_api_questions(posts_df=posts_df, tag=tag)
    answers_df = process_stack_api_answers(posts_df=posts_df)
    return combine_stack_dfs(questions_df=questions_df, answers_df=answers_df, tag=tag)
