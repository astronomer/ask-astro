from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from langchain import LLMChain
from langchain.callbacks.manager import Callbacks
from langchain.chains import ConversationalRetrievalChain
from langchain.chains.combine_documents.stuff import StuffDocumentsChain
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
from langchain_core.documents import Document
from langchain_core.prompts import format_document

from ask_astro.chains.custom_llm_filter_prompt import custom_llm_chain_filter_prompt_template
from ask_astro.chains.custom_llm_output_lines_parser import CustomLineListOutputParser
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

with open("ask_astro/templates/combine_docs_sys_prompt_webapp.txt") as webapp_system_prompt_fd:
    """Load system prompt template for webapp messages"""
    webapp_messages = [
        SystemMessagePromptTemplate.from_template(webapp_system_prompt_fd.read()),
        MessagesPlaceholder(variable_name="messages"),
        HumanMessagePromptTemplate.from_template("{question}"),
    ]

with open("ask_astro/templates/combine_docs_sys_prompt_slack.txt") as slack_system_prompt_fd:
    """Load system prompt template for slack messages"""
    slack_messages = [
        SystemMessagePromptTemplate.from_template(slack_system_prompt_fd.read()),
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
    create_schema_if_missing=WeaviateConfig.create_schema_if_missing,
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
        **AzureOpenAIParams.us_east2,
        deployment_name=MULTI_QUERY_RETRIEVER_DEPLOYMENT_NAME,
        temperature=MULTI_QUERY_RETRIEVER_TEMPERATURE,
    ),
    include_original=True,
    prompt=user_question_rewording_prompt_template,
    retriever=hybrid_retriever,
)
# Override the default LineListOutputParser from LangChain
multi_query_retriever.llm_chain.output_parser = CustomLineListOutputParser(max_lines=2)


def compress_documents(
    self,
    documents: Sequence[Document],
    query: str,
    callbacks: Callbacks | None = None,
) -> Sequence[Document]:
    """
    Same function as the one in Langchain, overriding to add relevance score thresholding
    """
    if len(documents) == 0:  # to avoid empty api call
        return []
    doc_list = list(documents)
    _docs = [d.page_content for d in doc_list]
    results = self.client.rerank(model=self.model, query=query, documents=_docs, top_n=self.top_n)
    final_results = []
    for r in results:
        doc = doc_list[r.index]
        if r.relevance_score < CohereConfig.min_relevance_score:
            continue
        doc.metadata["relevance_score"] = r.relevance_score
        final_results.append(doc)
    return final_results


# Rerank
CohereRerank.compress_documents = compress_documents
cohere_reranker_compressor = CohereRerank(
    model="rerank-english-v3.0", user_agent="langchain", top_n=CohereConfig.rerank_top_n
)

reranker_retriever = ContextualCompressionRetriever(
    base_compressor=cohere_reranker_compressor, base_retriever=multi_query_retriever
)

# GPT-3.5 to check over relevancy of the remaining documents
llm_chain_filter = LLMChainFilter.from_llm(
    AzureChatOpenAI(
        **AzureOpenAIParams.us_east2,
        deployment_name=CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_DEPLOYMENT_NAME,
        temperature=0.0,
    ),
    custom_llm_chain_filter_prompt_template,
)
llm_chain_filter_compression_retriever = ContextualCompressionRetriever(
    base_compressor=llm_chain_filter, base_retriever=reranker_retriever
)


# customize how the documents are combined to the final LLM call, overriding LangChain's default
def custom_combine_docs_override(self, docs: list[Document], **kwargs: Any) -> dict:
    # same function as the one in stuff doc chain, just changing this one line for doc number
    doc_strings = [
        f"===Beginning of Document===\nDocument {i+1}:\n" + format_document(doc, self.document_prompt)
        for i, doc in enumerate(docs)
    ]

    inputs = {k: v for k, v in kwargs.items() if k in self.llm_chain.prompt.input_variables}
    inputs[self.document_variable_name] = self.document_separator.join(doc_strings)
    return inputs


custom_document_combine_prompt = PromptTemplate(
    input_variables=["page_content", "docLink"],
    template="Document Link: {docLink}\n{page_content}\n===End of Document===\n",
)
StuffDocumentsChain._get_inputs = custom_combine_docs_override

custom_stuff_docs_chain_webapp: StuffDocumentsChain = load_qa_chain(
    AzureChatOpenAI(
        **AzureOpenAIParams.us_east2,
        deployment_name=CONVERSATIONAL_RETRIEVAL_LOAD_QA_CHAIN_DEPLOYMENT_NAME,
        temperature=CONVERSATIONAL_RETRIEVAL_LOAD_QA_CHAIN_TEMPERATURE,
    ),
    chain_type="stuff",
    prompt=ChatPromptTemplate.from_messages(webapp_messages),
)
custom_stuff_docs_chain_webapp.document_prompt = custom_document_combine_prompt

custom_stuff_docs_chain_slack: StuffDocumentsChain = load_qa_chain(
    AzureChatOpenAI(
        **AzureOpenAIParams.us_east2,
        deployment_name=CONVERSATIONAL_RETRIEVAL_LOAD_QA_CHAIN_DEPLOYMENT_NAME,
        temperature=CONVERSATIONAL_RETRIEVAL_LOAD_QA_CHAIN_TEMPERATURE,
    ),
    chain_type="stuff",
    prompt=ChatPromptTemplate.from_messages(slack_messages),
)
custom_stuff_docs_chain_slack.document_prompt = custom_document_combine_prompt

# Set up a ConversationalRetrievalChain to generate answers using the retriever.
webapp_answer_question_chain = ConversationalRetrievalChain(
    retriever=llm_chain_filter_compression_retriever,
    return_source_documents=True,
    question_generator=LLMChain(
        llm=AzureChatOpenAI(
            **AzureOpenAIParams.us_east2,
            deployment_name=CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_DEPLOYMENT_NAME,
            temperature=CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_TEMPERATURE,
        ),
        prompt=CONDENSE_QUESTION_PROMPT,
    ),
    combine_docs_chain=custom_stuff_docs_chain_webapp,
)

slack_answer_question_chain = ConversationalRetrievalChain(
    retriever=llm_chain_filter_compression_retriever,
    return_source_documents=True,
    question_generator=LLMChain(
        llm=AzureChatOpenAI(
            **AzureOpenAIParams.us_east2,
            deployment_name=CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_DEPLOYMENT_NAME,
            temperature=CONVERSATIONAL_RETRIEVAL_LLM_CHAIN_TEMPERATURE,
        ),
        prompt=CONDENSE_QUESTION_PROMPT,
    ),
    combine_docs_chain=custom_stuff_docs_chain_slack,
)
