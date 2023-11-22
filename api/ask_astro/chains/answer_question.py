from __future__ import annotations

from typing import Any

from langchain import LLMChain
from langchain.callbacks.manager import (
    CallbackManagerForChainRun,
)
from langchain.chains import ConversationalRetrievalChain
from langchain.chains.conversational_retrieval.prompts import CONDENSE_QUESTION_PROMPT
from langchain.chains.question_answering import load_qa_chain
from langchain.chat_models import AzureChatOpenAI
from langchain.prompts import (
    ChatPromptTemplate,
    HumanMessagePromptTemplate,
    MessagesPlaceholder,
    SystemMessagePromptTemplate,
)
from langchain.retrievers import MultiQueryRetriever
from langchain.retrievers.document_compressors import CohereRerank
from langchain.retrievers.weaviate_hybrid_search import WeaviateHybridSearchRetriever

from ask_astro.clients.weaviate_ import client
from ask_astro.config import AzureOpenAIParams, CohereConfig, WeaviateConfig
from ask_astro.settings import (
    CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_DEPLOYMENT_NAME,
    CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_TEMPERATURE,
    CONVERSATIONAL_RETRIEVAL_LOAD_QA_CHAIN_DEPLOYMENT_NAME,
    CONVERSATIONAL_RETRIEVAL_LOAD_QA_CHAIN_TEMPERATURE,
    MULTI_QUERY_RETRIEVER_DEPLOYMENT_NAME,
    MULTI_QUERY_RETRIEVER_TEMPERATURE,
)

with open("ask_astro/templates/combine_docs_chat_prompt.txt") as system_prompt_fd:
    """Load system prompt template from a file and structure it."""
    messages = [
        SystemMessagePromptTemplate.from_template(system_prompt_fd.read()),
        MessagesPlaceholder(variable_name="messages"),
        HumanMessagePromptTemplate.from_template("{question}"),
    ]

# Initialize a MultiQueryRetriever using AzureChatOpenAI and Weaviate.
retriever = MultiQueryRetriever.from_llm(
    llm=AzureChatOpenAI(
        **AzureOpenAIParams.us_east,
        deployment_name=MULTI_QUERY_RETRIEVER_DEPLOYMENT_NAME,
        temperature=MULTI_QUERY_RETRIEVER_TEMPERATURE,
    ),
    retriever=WeaviateHybridSearchRetriever(
        client=client,
        index_name=WeaviateConfig.index_name,
        text_key=WeaviateConfig.text_key,
        attributes=WeaviateConfig.attributes,
        create_schema_if_missing=WeaviateConfig.create_schema_if_missing,
        k=WeaviateConfig.k,
        alpha=WeaviateConfig.alpha,
    ),
)


class AskAstroCoversationalRetrievalChainChain(ConversationalRetrievalChain):
    def _get_docs(
        self,
        question: str,
        inputs: dict[str, Any],
        *,
        run_manager: CallbackManagerForChainRun,
    ):
        docs = super()._get_docs(question=question, inputs=inputs, run_manager=run_manager)
        if CohereConfig.cohere_api_key:
            compressor = CohereRerank(top_n=CohereConfig.top_n, user_agent="langchain")
            return compressor.compress_documents(docs, question)
        return docs


# Set up a AskAstroCoversationalRetrievalChainChain to generate answers using the retriever.
answer_question_chain = AskAstroCoversationalRetrievalChainChain(
    retriever=retriever,
    return_source_documents=True,
    question_generator=LLMChain(
        llm=AzureChatOpenAI(
            **AzureOpenAIParams.us_east,
            deployment_name=CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_DEPLOYMENT_NAME,
            temperature=CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_TEMPERATURE,
        ),
        prompt=CONDENSE_QUESTION_PROMPT,
    ),
    combine_docs_chain=load_qa_chain(
        AzureChatOpenAI(
            **AzureOpenAIParams.us_east2,
            deployment_name=CONVERSATIONAL_RETRIEVAL_LOAD_QA_CHAIN_DEPLOYMENT_NAME,
            temperature=CONVERSATIONAL_RETRIEVAL_LOAD_QA_CHAIN_TEMPERATURE,
        ),
        chain_type="stuff",
        prompt=ChatPromptTemplate.from_messages(messages),
    ),
)
