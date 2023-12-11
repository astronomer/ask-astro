import requests
import pandas as pd

source = {
        "base_url": "https://forum.astronomer.io",
        "trust_level_cutoff": 3,
        "exclude_categories": [
            {'name': 'Site Feedback', 'id': 3},
            {'name': 'Nebula', 'id': 6},
            {'name': 'Software', 'id': 5},
            ]
    }

post_columns = [
    "id",
    "topic_id",
    # "topic_slug",
    "score",
    "cooked",
    "user_deleted",
    # "accepted_answer",
    # "topic_accepted_answer",
    "post_number",
    "trust_level"
]

exclude_category_ids = [cat["id"] for cat in source["exclude_categories"]]

response = requests.get(source["base_url"] + "/categories.json?include_subcategories=True")
if response.ok:
    parent_categories = response.json()["category_list"]["categories"]
    parent_category_ids = [
        category["id"] for category in parent_categories if category["id"] not in exclude_category_ids
    ]

topics = []
for category in parent_category_ids:
    print(category)
    response = requests.get(source["base_url"] + f"/c/{category}.json")
    if response.ok:
        response = response.json()["topic_list"]
        topics += [
            topic for topic in response["topics"] if topic["category_id"] not in exclude_category_ids
        ]
        while response.get("more_topics_url"):
            next_page = response["more_topics_url"].replace("?", ".json?")
            print(next_page)
            response = requests.get(source["base_url"] + next_page)
            if response.ok:
                response = response.json()["topic_list"]
                topics += [
                    topic for topic in response["topics"] if topic["category_id"] not in exclude_category_ids
                ]

topic_ids = {topic["id"] for topic in topics}

posts = []
for topic_id in topic_ids:
    response = requests.get(source["base_url"] + f"/t/{topic_id}.json")
    if response.ok:

        response = response.json()
        post_ids = response["post_stream"]["stream"]
        print(post_ids)
        post_ids_query = "".join([f"post_ids[]={id}&" for id in post_ids])

        response = requests.get(source["base_url"] + f"/t/{topic_id}/posts.json?" + post_ids_query)
        if response.ok:
            response = response.json()
            posts += response["post_stream"]["posts"]

posts_df = pd.DataFrame(posts)[post_columns]

score_cutoff = posts_df[posts_df.post_number == 1].score.quantile(.75)

first_posts_df = posts_df[
    (posts_df.post_number == 1) & (posts_df.score >= score_cutoff)
    ]
first_posts_df = first_posts_df[["topic_id", "cooked"]].rename({"cooked": "first_post"}, axis=1)

# remove first post for joining later
posts_df.drop(posts_df[posts_df.post_number == 1].index, inplace=True)

# select only posts with high trust level
posts_df.drop(posts_df[posts_df.trust_level < source["trust_level_cutoff"]].index, inplace=True)

posts_df.sort_values("post_number", inplace=True)

posts_df = posts_df.groupby("topic_id")["cooked"].apply("".join).reset_index()

posts_df = posts_df.merge(first_posts_df)

posts_df["content"] = posts_df.apply(lambda x: x.first_post + x.cooked, axis=1)
posts_df["docLink"] = posts_df.topic_id.apply(lambda x: source["base_url"] + f"/t/{x}/1")

posts_df = posts_df[["content", "docLink"]]

print(posts_df)