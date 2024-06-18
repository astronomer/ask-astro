<p align="center">
  <img src="_static/logo.svg" />
</p>

<br />

Ask Astro is an open-source reference implementation of [Andreessen Horowitz's LLM Application Architecture](https://a16z.com/emerging-architectures-for-llm-applications/) built by [Astronomer](https://astronomer.io). It provides an end-to-end example of a Q&A LLM application used to answer questions about Apache Airflow and Astronomer, including:

- Airflow DAGs for data ingestion from various sources to a vector database
- An API containing the business logic of taking a user's prompt and ultimately generating a response
- A Slack bot that exposes Ask Astro in Slack workspaces
- A UI that exposes Ask Astro to the public
- Airflow DAGs that process user feedback and generate AI feedback on questions and answers

These are generally divided into three categories: data retrieval & embedding, prompt orchestration, and feedback loops. The rest of this README contains more in-depth detail on each of the categories in advance of a series of blog posts that will be written about the topics.

If you have any questions, feedback, or want to share similar use cases, reach out to ai@astronomer.io.

## Data Retrieval & Embedding
<p align="center">
  <img src="_static/ingestion.png" />
</p>

In order to make the responses as factual and accurate as possible, it's generally best practice to use [Retrieval Augmented Generation (RAG)](https://proceedings.neurips.cc/paper/2020/hash/6b493230205f780e1bc26945df7481e5-Abstract.html). However, in order for RAG to be effective, a vector database needs to be populated with the most up-to-date and relevant information.

Ask Astro uses a set of Airflow DAGs that: ingest data from a source via an API or Python library, preprocesses and splits the data into smaller chunks, embeds those chunks, and writes the embeddings to Weaviate. As of today, Ask Astro retrieves data from the following sources:

- [Apache Airflow docs](https://airflow.apache.org/docs/)
- [Astronomer docs](https://docs.astronomer.io)
- [Astronomer blog](https://www.astronomer.io/blog/)
- [Astronomer Registry](https://registry.astronomer.io)
- [Apache Airflow GitHub](https://github.com/apache/airflow) pull requests
- [OpenLineage GitHub](https://github.com/OpenLineage/OpenLineage)
- [OpenLineage GitHub docs](https://github.com/OpenLineage/docs)
- [StackOverflow's Stack Exchange Data Dump](https://archive.org/details/stackexchange)

Generally, each of these sources has a DAG that handles the ingestion flow. We use LangChain's built-in text splitters for processing Markdown, RST, and Python code into smaller chunks to ensure each document is small enough to give accurate results when doing embeddings. We then use a Weaviate provider that we've built (and plan to publish) to both embed and store each document as a vector in Weaviate using OpenAI's embedding model.

In addition to the individual DAGs per source, we have one DAG to do full-database refreshes based on a baseline of all data. The first time the `ask-astro-load-bulk` DAG runs it saves extracted documents in parquet files for a point-in-time baseline.  This baseline allows us to experiment with new vector databases, embedding models, chunking strategies, etc. much more quickly.

See the [Ingest README](https://github.com/astronomer/ask-astro/tree/main/airflow/README.md) for details on configuring ingest with sources and connection details.

## Prompt Orchestration
<p align="center">
  <img src="_static/prompt-orchestration.png" />
</p>

Ask Astro uses LangChain's `ConversationalRetrievalChain` to generate a response. This chain does the following:

1. Use an LLM to generate 2 variations of the original user prompt with different wording. This is to ensure we retrieve as much helpful context as possible from our vector database
2. Embed each of 2 reworded prompts + original user prompt with OpenAI's embeddings model
3. Retrieve up to 100 documents for each prompt from Weaviate using the embedded vectors from each prompt
4. Using Cohere Reranker to rerank the combined up to 300 candidate pool of documents to only 8 most relevant documents
5. Use a fast and cheapt LLM (`gpt-3.5-turbo`) to check relevancy of each of the 8 documents.
6. Combine the original user prompt with most relevant sources found, and make a final LLM call to generate an answer

This generally works well. For prompt rewording, we use `gpt-3.5-turbo`, which runs very quickly and inexpensively. For the actual user-facing answer generation, we use `gpt-4o` to ensure high quality answers.


## Feedback Loops
<p align="center">
  <img src="_static/feedback-loops.png" />
</p>

Airflow is critical in improving model performance over time. Feedback on answers come from two places:

- End users can explicitly supply feedback by rating an answer as correct or incorrect
- LLMs rate each question and answer according to it's "helpfulness", "publicness", and "on-topicness"

If a user provides feedback that the answer is correct, and the LLM rates the answer as helpful, public, and on-topic, Ask Astro (1) marks the answer as a good example to be displayed on the Ask Astro homepage for users to derive inspiration from, and (2) writes the question and answer back to Weaviate as a potential source to be used in future prompts. This way, there's a continuous feedback loop to constantly improve the model over time.

## Future Exploration

A project like Ask Astro is never "complete", and there are always more methods and use cases to explore. Further exploration in Ask Astro (and more generally, in using Airflow for LLM operations) may come from the following areas:

- **Data Privacy & Private Context**: can the model be supplemented with customer-specific data to provide more accurate, contextual answers?
- **Fine Tuning**: are there any performance or accuracy improvements that come from fine tuning a model?
- **Semantic Caching**: if there are two similar questions asked to Ask Astro, can we avoid expensive gpt-4 calls?
- **Feedback Loops**: what else can we do with information about the accuracy of answers? For example, can we infer source usefulness based on how frequently (or infrequently) a source is used in accurate answers?
- **Experimentation**: how do we effectively experiment and measure accuracy or performance improvements as a result of experiments?
- **LLM Operations and Observability**: how do we ensure we have good visibility into what the model's doing, particularly given that it's making multiple API & LLM calls?


## Local dev env

You can use local dev [script](../scripts/local_dev.py) to start UI, API server and API

````bash
python3 scripts/local_dev.py run-api-server # To run backend
python3 scripts/local_dev.py run-ui         # To run UI
python3 scripts/local_dev.py run-airflow    # To run Airflow
````

**Backend API sever**

- [Run ask-astro API server](./api/README.md)

**User Interface**

- [Run ask-astro UI](./ui/README.md)

**Apache Airflow**

- [Run airflow](./airflow/README.md)

## Deployment
The following sections describe how to deploy the various components of Ask Astro.

### Backend API
- [Backend API](./api/cloudbuild_and_run.md)
- [Google Firestore](./api/google_firestore.md)

### User Interface
- [UI](./ui/README.md)

### Apache Airflow
- [Airflow](./airflow/README.md)

## Slackbot
- [Slackbot](./api/setup_slack_bot.md)
