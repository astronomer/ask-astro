from __future__ import annotations

from langchain import LLMChain
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

from ask_astro.clients.weaviate_ import docsearch
from ask_astro.config import AzureOpenAIParams
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
    retriever=docsearch.as_retriever(),
)

# Set up a ConversationalRetrievalChain to generate answers using the retriever.
answer_question_chain = ConversationalRetrievalChain(
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
