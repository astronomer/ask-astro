from datetime import datetime
from textwrap import dedent

import pandas as pd
from stackapi import StackAPI
from weaviate.util import generate_uuid5

question_template = dedent(
    """
    TITLE: {title}
    DATE: {date}
    BY: {user}
    SCORE: {score}
    {body}{question_comments}"""
)

answer_template = dedent(
    """
    DATE: {date}
    BY: {user}
    SCORE: {score}
    {body}{answer_comments}"""
)

comment_template = "\n{user} on {date} [Score: {score}]: {body}\n"


def fetch_questions_through_stack_api(
    tag: str, stackoverflow_cutoff_date: str, *, page_size: int = 100, max_pages: int = 10000000
) -> dict:
    """Fetch data from stackoverflow site through stack api"""
    fromdate = datetime.strptime(stackoverflow_cutoff_date, "%Y-%m-%d")
    first_question_id, first_question_creation_date = fetch_first_question_after_fromdate(tag=tag, fromdate=fromdate)

    stack_api = StackAPI(name="stackoverflow", page_size=page_size, max_pages=max_pages)

    # https://api.stackexchange.com/docs/read-filter#filters=!-(5KXGCFLp3w9.-7QsAKFqaf5yFPl**9q*_hsHzYGjJGQ6BxnCMvDYijFE&filter=default&run=true
    filter_ = "!-(5KXGCFLp3w9.-7QsAKFqaf5yFPl**9q*_hsHzYGjJGQ6BxnCMvDYijFE"

    questions_resp = stack_api.fetch(
        endpoint="questions",
        filter=filter_,
        tagged=tag,
        fromdate=fromdate,
        order="desc",
        sort="creation",
    )
    questions = questions_resp.pop("items")
    while questions_resp["quota_remaining"] > 0 and questions[-1]["question_id"] != first_question_id:
        todate = questions[-1]["creation_date"]
        questions_resp = stack_api.fetch(
            endpoint="questions",
            filter=filter_,
            tagged=tag,
            fromdate=fromdate,
            todate=todate,
            order="desc",
            sort="creation",
        )
        questions.extend(questions_resp.pop("items"))

    return questions


def fetch_first_question_after_fromdate(*, tag: str, fromdate: datetime.date) -> tuple[int, int]:
    """Get the first question id after fromdate"""
    stack_api = StackAPI(name="stackoverflow")
    filter_ = "!*1PUVE3_-UtLf0rvavrile9fyVsn*T)jdVaO6_P)K"
    questions_resp = stack_api.fetch(
        endpoint="questions",
        page=1,
        pagesize=1,
        order="asc",
        sort="creation",
        fromdate=fromdate,
        tagged=tag,
        filter=filter_,
    )
    first_question = questions_resp["items"][0]

    return first_question["question_id"], first_question["creation_date"]


def process_stack_api_posts(questions: dict) -> pd.DataFrame:
    """
    This helper function processes questions pulled from slack api endpoint into a set format.

    param questions: a dict of questions pulled from stack API
    type questions: dict
    """
    posts_df = pd.DataFrame(questions)
    posts_df = posts_df[posts_df["answer_count"] >= 1]
    posts_df = posts_df[posts_df["score"] >= 1]
    posts_df.reset_index(inplace=True, drop=True)
    return posts_df


def process_stack_api_questions(posts_df: pd.DataFrame, tag: str) -> pd.DataFrame:
    """
    This helper function processes a dataframe of slack posts into a set format.

    param posts_df: a dataframe with stack overflow posts from an stack API
    type posts_df: pd.DataFrame

    """
    questions_df = posts_df
    questions_df["comments"] = questions_df["comments"].fillna("")
    questions_df["question_comments"] = questions_df["comments"].apply(lambda x: process_stack_api_comments(x))

    # format question content
    questions_df["question_text"] = questions_df.apply(
        lambda x: question_template.format(
            title=x.title,
            user=x.owner.get("user_id", ""),
            date=datetime.fromtimestamp(x.creation_date).strftime("%Y-%m-%d"),
            score=x.score,
            body=x.body_markdown,
            question_comments=x.question_comments,
        ),
        axis=1,
    )
    questions_df = questions_df[["link", "question_id", "question_text"]].set_index("question_id")
    questions_df["docSource"] = f"stackoverflow {tag}"
    questions_df.rename({"link": "docLink", "question_text": "content"}, axis=1, inplace=True)
    return questions_df


def process_stack_api_comments(comments: list) -> str:
    """
    This helper function processes a list of slack comments for a question or answer

    param comments: a list of stack overflow comments from the api
    type comments: list

    """
    return "".join(
        [
            comment_template.format(
                user=comment["owner"].get("user_id", ""),
                date=datetime.fromtimestamp(comment["creation_date"]).strftime("%Y-%m-%d"),
                score=comment["score"],
                body=comment["body_markdown"],
            )
            for comment in comments
        ]
    )


def process_stack_api_answers(posts_df: pd.DataFrame) -> pd.DataFrame:
    """
    This helper function builds a dataframe of slack answers based on posts.

    The column answer_text is created in markdown format based on answer_template.

    param posts_df: a dataframe with stack overflow posts from an stack API
    type posts_df: pd.DataFrame

    """

    answers_df = posts_df.explode("answers").reset_index(drop=True)
    answers_df["comments"] = answers_df["answers"].apply(lambda x: x.get("comments"))
    answers_df["comments"] = answers_df["comments"].fillna("")
    answers_df["answer_comments"] = answers_df["comments"].apply(lambda x: process_stack_api_comments(x))

    answers_df["answer_text"] = answers_df[["answers", "answer_comments"]].apply(
        lambda x: answer_template.format(
            score=x.answers["score"],
            date=datetime.fromtimestamp(x.answers["creation_date"]).strftime("%Y-%m-%d"),
            user=x.answers["owner"].get("user_id", ""),
            body=x.answers["body_markdown"],
            answer_comments=x.answer_comments,
        ),
        axis=1,
    )
    answers_df = answers_df.groupby("question_id")["answer_text"].apply(lambda x: "".join(x))
    return answers_df


def combine_stack_dfs(*, questions_df: pd.DataFrame, answers_df: pd.DataFrame, tag: str) -> pd.DataFrame:
    """This helper function builds a dataframe based on posts and answers."""
    # Join questions with answers
    df = questions_df.join(answers_df)
    df = df.apply(
        lambda x: pd.Series([f"stackoverflow {tag}", x.docLink, "\n".join([x.content, x.answer_text])]),
        axis=1,
    )
    df.columns = ["docSource", "docLink", "content"]

    df.reset_index(inplace=True, drop=True)
    df["sha"] = df.apply(generate_uuid5, axis=1)

    # column order matters for uuid generation
    df = df[["docSource", "sha", "content", "docLink"]]
    return df
