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
from langchain.prompts.prompt import PromptTemplate
from langchain.retrievers import ContextualCompressionRetriever, MultiQueryRetriever
from langchain.retrievers.document_compressors import CohereRerank, LLMChainFilter
from langchain.retrievers.weaviate_hybrid_search import WeaviateHybridSearchRetriever

from ask_astro.chains.custom_llm_filter_prompt import custom_llm_chain_filter_prompt_template
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

hybrid_retriever = WeaviateHybridSearchRetriever(
    client=client,
    index_name=WeaviateConfig.index_name,
    text_key=WeaviateConfig.text_key,
    attributes=WeaviateConfig.attributes,
    k=WeaviateConfig.k,
    alpha=WeaviateConfig.alpha,
)

# Initialize a MultiQueryRetriever using AzureChatOpenAI and Weaviate.
user_question_rewording_prompt_template = PromptTemplate(
    input_variables=["question"],
    template="""You are an AI language model assistant. Your task is
    to generate 2 different versions of the given user
    question to retrieve relevant documents from a vector database.
    By rewording the original question, expanding on abbreviated words if there are any,
    and generating multiple perspectives on the user question,
    your goal is to help the user overcome some of the limitations
    of distance-based similarity search. Provide these alternative
    questions separated by newlines. Original question: {question}""",
)
multi_query_retriever = MultiQueryRetriever.from_llm(
    llm=AzureChatOpenAI(
        **AzureOpenAIParams.us_east,
        deployment_name=MULTI_QUERY_RETRIEVER_DEPLOYMENT_NAME,
        temperature=MULTI_QUERY_RETRIEVER_TEMPERATURE,
    ),
    include_original=True,
    prompt=user_question_rewording_prompt_template,
    retriever=hybrid_retriever,
)

# Rerank
cohere_reranker_compressor = CohereRerank(user_agent="langchain", top_n=CohereConfig.rerank_top_n)
reranker_retriever = ContextualCompressionRetriever(
    base_compressor=cohere_reranker_compressor, base_retriever=multi_query_retriever
)

# GPT-3.5 to check over relevancy of the remaining documents
llm_chain_filter = LLMChainFilter.from_llm(
    AzureChatOpenAI(
        **AzureOpenAIParams.us_east,
        deployment_name=CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_DEPLOYMENT_NAME,
        temperature=0.0,
    ),
    custom_llm_chain_filter_prompt_template,
)
llm_chain_filter_compression_retriever = ContextualCompressionRetriever(
    base_compressor=llm_chain_filter, base_retriever=reranker_retriever
)

# Set up a ConversationalRetrievalChain to generate answers using the retriever.
answer_question_chain = ConversationalRetrievalChain(
    retriever=llm_chain_filter_compression_retriever,
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
