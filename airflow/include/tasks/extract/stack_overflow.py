from __future__ import annotations

import datetime

import pandas as pd
from stackapi import StackAPI
from weaviate.util import generate_uuid5

from include.tasks.extract.utils.stack_overflow_helpers import (
    process_stack_answers,
    process_stack_answers_api,
    process_stack_comments,
    process_stack_comments_api,
    process_stack_posts,
    process_stack_questions,
    process_stack_questions_api,
)


def extract_stack_overflow_archive(tag: str, stackoverflow_cutoff_date: str) -> pd.DataFrame:
    """
    This task generates stack overflow documents as a single markdown document per question with associated comments
    and answers.  The task returns a pandas dataframe with all documents.  The archive data was pulled from
    the internet archives and processed to local files for ingest.

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

    posts_df = pd.read_parquet("include/data/stack_overflow/posts/posts.parquet")

    posts_df = process_stack_posts(posts_df=posts_df, stackoverflow_cutoff_date=stackoverflow_cutoff_date)

    comments_df = pd.concat(
        [
            pd.read_parquet("include/data/stack_overflow/comments/comments_0.parquet"),
            pd.read_parquet("include/data/stack_overflow/comments/comments_1.parquet"),
        ],
        ignore_index=True,
    )

    comments_df = process_stack_comments(comments_df=comments_df)

    questions_df = process_stack_questions(posts_df=posts_df, comments_df=comments_df, tag=tag)

    answers_df = process_stack_answers(posts_df=posts_df, comments_df=comments_df)

    # Join questions with answers
    df = questions_df.join(answers_df)
    df = df.apply(
        lambda x: pd.Series([f"stackoverflow {tag}", x.docLink, "\n".join([x.content, x.answer_text])]), axis=1
    )
    df.columns = ["docSource", "docLink", "content"]

    df.reset_index(inplace=True, drop=True)
    df["sha"] = df.apply(generate_uuid5, axis=1)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return df


def extract_stack_overflow(tag: str, stackoverflow_cutoff_date: str) -> pd.DataFrame:
    """
    This task generates stack overflow documents as a single markdown document per question with associated comments
    and answers.  The task returns a pandas dataframe with all documents.

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

    SITE = StackAPI(name="stackoverflow", max_pagesize=100, max_pages=10000000)

    fromdate = datetime.datetime.strptime(stackoverflow_cutoff_date, "%Y-%m-%d")

    # https://api.stackexchange.com/docs/read-filter#filters=!-(5KXGCFLp3w9.-7QsAKFqaf5yFPl**9q*_hsHzYGjJGQ6BxnCMvDYijFE&filter=default&run=true
    filter_ = "!-(5KXGCFLp3w9.-7QsAKFqaf5yFPl**9q*_hsHzYGjJGQ6BxnCMvDYijFE"

    questions_dict = SITE.fetch(endpoint="questions", tagged=tag, fromdate=fromdate, filter=filter_)
    items = questions_dict.pop("items")

    # TODO: check if we need to paginate
    len(items)
    # TODO: add backoff logic.  For now just fail the task if we can't fetch all results due to api rate limits.
    assert not questions_dict["has_more"]

    posts_df = pd.DataFrame(items)
    posts_df = posts_df[posts_df["answer_count"] >= 1]
    posts_df = posts_df[posts_df["score"] >= 1]
    posts_df.reset_index(inplace=True, drop=True)

    # process questions
    questions_df = posts_df
    questions_df["comments"] = questions_df["comments"].fillna("")
    questions_df["question_comments"] = questions_df["comments"].apply(lambda x: process_stack_comments_api(x))
    questions_df = process_stack_questions_api(questions_df=questions_df, tag=tag)

    # process associated answers
    answers_df = posts_df.explode("answers").reset_index(drop=True)
    answers_df["comments"] = answers_df["answers"].apply(lambda x: x.get("comments"))
    answers_df["comments"] = answers_df["comments"].fillna("")
    answers_df["answer_comments"] = answers_df["comments"].apply(lambda x: process_stack_comments_api(x))
    answers_df = process_stack_answers_api(answers_df=answers_df)

    # combine questions and answers
    df = questions_df.join(answers_df).reset_index(drop=True)
    df["content"] = df[["question_text", "answer_text"]].apply("\n".join, axis=1)

    df["sha"] = df.apply(generate_uuid5, axis=1)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]

    return df
