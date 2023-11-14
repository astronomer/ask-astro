import datetime
from textwrap import dedent

import pandas as pd
from html2text import html2text

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

post_types = {
    "1": "Question",
    "2": "Answer",
    "3": "Wiki",
    "4": "TagWikiExcerpt",
    "5": "TagWiki",
    "6": "ModeratorNomination",
    "7": "WikiPlaceholder",
    "8": "PrivilegeWiki",
}

posts_columns = {
    "_COL_0": "post_id",
    "_COL_1": "type",
    "_COL_3": "parent_id",
    "_COL_4": "created_on",
    "_COL_6": "score",
    "_COL_8": "body",
    "_COL_9": "user_id",
    "_COL_10": "user_name",
    "_COL_15": "title",
    "_COL_17": "answer_count",
}

comments_columns = {
    "_COL_0": "comment_id",
    "_COL_1": "post_id",
    "_COL_2": "comment_score",
    "_COL_3": "comment_body",
    "_COL_4": "comment_created_on",
    "_COL_5": "comment_user_name",
    "_COL_6": "comment_user_id",
}


def process_stack_posts(posts_df: pd.DataFrame, stackoverflow_cutoff_date: str) -> pd.DataFrame:
    """
    This helper function processes a dataframe of slack posts into a set format.

    param posts_df: a dataframe with stack overflow posts from an archive
    type posts_df: pd.DataFrame

    param stackoverflow_cutoff_date: Only messages from after this date will be extracted.
    type stackoverflow_cutoff_date: str

    """
    posts_df = posts_df[posts_columns.keys()]

    posts_df.rename(posts_columns, axis=1, inplace=True)
    posts_df["type"] = posts_df["type"].apply(lambda x: post_types[x])
    posts_df["created_on"] = pd.to_datetime(posts_df["created_on"])

    posts_df["post_id"] = posts_df["post_id"].astype(str)
    posts_df["parent_id"] = posts_df["parent_id"].astype(str)
    posts_df["user_id"] = posts_df["user_id"].astype(str)
    posts_df["user_name"] = posts_df["user_name"].astype(str)

    posts_df = posts_df[posts_df["created_on"] >= stackoverflow_cutoff_date]
    posts_df["user_id"] = posts_df.apply(lambda x: x.user_id or x.user_name or "Unknown User", axis=1)
    posts_df.reset_index(inplace=True, drop=True)

    return posts_df


def process_stack_comments(comments_df: pd.DataFrame) -> pd.DataFrame:
    """
    This helper function processes a dataframe of slack comments into a set format.

    param comments_df: a dataframe with stack overflow comments from an archive
    type comments_df: pd.DataFrame

    """
    comments_df = comments_df[comments_columns.keys()]

    comments_df.rename(comments_columns, axis=1, inplace=True)
    comments_df["comment_created_on"] = pd.to_datetime(comments_df["comment_created_on"])
    comments_df["comment_user_id"] = comments_df.apply(
        lambda x: x.comment_user_id or x.comment_user_name or "Unknown User", axis=1
    )

    comments_df["post_id"] = comments_df["post_id"].astype(str)
    comments_df["comment_user_id"] = comments_df["comment_user_id"].astype(str)
    comments_df["comment_user_name"] = comments_df["comment_user_name"].astype(str)
    comments_df[["comment_score"]] = comments_df[["comment_score"]].astype(int)

    comments_df["comment_text"] = comments_df.apply(
        lambda x: comment_template.format(
            user=x.comment_user_id, date=x.comment_created_on, score=x.comment_score, body=x.comment_body
        ),
        axis=1,
    )
    comments_df = comments_df[["post_id", "comment_text"]].groupby("post_id").agg(list)
    comments_df["comment_text"] = comments_df["comment_text"].apply(lambda x: "\n".join(x))
    comments_df.reset_index(inplace=True)

    return comments_df


def process_stack_questions(posts_df: pd.DataFrame, comments_df: pd.DataFrame, tag: str) -> pd.DataFrame:
    """
    This helper function builds a dataframe of slack questions based on posts and comments.

    The column question_text is created in markdown format based on question_template.

    """
    questions_df = posts_df[posts_df["type"] == "Question"]
    questions_df = questions_df.drop("parent_id", axis=1)
    questions_df.rename({"body": "question_body", "post_id": "question_id"}, axis=1, inplace=True)
    questions_df["answer_count"] = questions_df["answer_count"].astype(int)
    questions_df["score"] = questions_df["score"].astype(int)
    questions_df = questions_df[questions_df["score"] >= 1]
    questions_df = questions_df[questions_df["answer_count"] >= 1]
    questions_df.reset_index(inplace=True, drop=True)

    questions_df = pd.merge(questions_df, comments_df, left_on="question_id", right_on="post_id", how="left")
    questions_df["comment_text"].fillna("", inplace=True)
    questions_df.drop("post_id", axis=1, inplace=True)
    questions_df["link"] = questions_df["question_id"].apply(lambda x: f"https://stackoverflow.com/questions/{x}")

    questions_df["question_text"] = questions_df.apply(
        lambda x: question_template.format(
            title=x.title,
            user=x.user_id,
            date=x.created_on,
            score=x.score,
            body=html2text(x.question_body),
            question_comments=x.comment_text,
        ),
        axis=1,
    )

    questions_df = questions_df[["link", "question_id", "question_text"]]
    questions_df = questions_df.set_index("question_id")
    questions_df["docSource"] = f"stackoverflow {tag}"
    questions_df = questions_df[["docSource", "link", "question_text"]]
    questions_df.columns = ["docSource", "docLink", "content"]

    return questions_df


def process_stack_answers(posts_df: pd.DataFrame, comments_df: pd.DataFrame) -> pd.DataFrame:
    """
    This helper function builds a dataframe of slack answers based on posts and comments.

    The column answer_text is created in markdown format based on answer_template.

    """
    answers_df = posts_df[posts_df["type"] == "Answer"]
    answers_df = answers_df[["created_on", "score", "user_id", "post_id", "parent_id", "body"]]
    answers_df.rename({"body": "answer_body", "post_id": "answer_id", "parent_id": "question_id"}, axis=1, inplace=True)
    answers_df.reset_index(inplace=True, drop=True)
    answers_df = pd.merge(answers_df, comments_df, left_on="answer_id", right_on="post_id", how="left")
    answers_df["comment_text"].fillna("", inplace=True)
    answers_df.drop("post_id", axis=1, inplace=True)
    answers_df["link"] = answers_df["question_id"].apply(lambda x: f"https://stackoverflow.com/questions/{x}")
    answers_df["answer_text"] = answers_df.apply(
        lambda x: answer_template.format(
            user=x.user_id,
            date=x.created_on,
            score=x.score,
            body=html2text(x.answer_body),
            answer_comments=x.comment_text,
        ),
        axis=1,
    )
    answers_df = answers_df.groupby("question_id")["answer_text"].apply(lambda x: "".join(x))

    return answers_df


def process_stack_comments_api(comments: list) -> str:
    """
    This helper function processes a list of slack comments for a question or answer

    param comments: a list of stack overflow comments from the api
    type comments: list

    """
    return "".join(
        [
            comment_template.format(
                user=comment["owner"]["user_id"],
                date=datetime.datetime.fromtimestamp(comment["creation_date"]).strftime("%Y-%m-%d"),
                score=comment["score"],
                body=comment["body_markdown"],
            )
            for comment in comments
        ]
    )


def process_stack_questions_api(questions_df: pd.DataFrame, tag: str) -> pd.DataFrame:
    """
    This helper function formats a questions dataframe pulled from slack api.

    The column question_text is created in markdown format based on question_template.

    """

    # format question content
    questions_df["question_text"] = questions_df.apply(
        lambda x: question_template.format(
            title=x.title,
            user=x.owner["user_id"],
            date=datetime.datetime.fromtimestamp(x.creation_date).strftime("%Y-%m-%d"),
            score=x.score,
            body=x.body_markdown,
            question_comments=x.question_comments,
        ),
        axis=1,
    )
    questions_df = questions_df[["link", "question_id", "question_text"]].set_index("question_id")

    questions_df["docSource"] = f"stackoverflow {tag}"

    questions_df.rename({"link": "docLink"}, axis=1, inplace=True)

    return questions_df


def process_stack_answers_api(answers_df: pd.DataFrame) -> pd.DataFrame:
    """
    This helper function formats answers into markdownd documents and returns
    a dataframe with question_id as index.
    """
    answers_df["answer_text"] = answers_df[["answers", "answer_comments"]].apply(
        lambda x: answer_template.format(
            score=x.answers["score"],
            date=datetime.datetime.fromtimestamp(x.answers["creation_date"]).strftime("%Y-%m-%d"),
            user=x.answers["owner"]["user_id"],
            body=x.answers["body_markdown"],
            answer_comments=x.answer_comments,
        ),
        axis=1,
    )
    answers_df = answers_df.groupby("question_id")["answer_text"].apply(lambda x: "".join(x))

    return answers_df
