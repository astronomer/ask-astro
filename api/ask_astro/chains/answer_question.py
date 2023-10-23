from ask_astro.clients.weaviate_ import docsearch
from ask_astro.config import AzureOpenAIParams
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

# Skip Slack as data source.
where_filter = {"path": ["docSource"], "operator": "NotEqual", "valueString": "troubleshooting"}

query = {
    "where": where_filter,
}

docsearch_results = docsearch.search(query=query, search_type="similarity")

with open("ask_astro/templates/combine_docs_chat_prompt.txt") as system_prompt_fd:
    messages = [
        SystemMessagePromptTemplate.from_template(system_prompt_fd.read()),
        MessagesPlaceholder(variable_name="messages"),
        HumanMessagePromptTemplate.from_template("{question}"),
    ]

retriever = MultiQueryRetriever.from_llm(
    llm=AzureChatOpenAI(
        **AzureOpenAIParams.us_east,
        deployment_name="gpt-35-turbo",
        temperature=0,
    ),
    retriever=docsearch_results.as_retriever(),
)

answer_question_chain = ConversationalRetrievalChain(
    retriever=retriever,
    return_source_documents=True,
    question_generator=LLMChain(
        llm=AzureChatOpenAI(
            **AzureOpenAIParams.us_east,
            deployment_name="gpt-35-turbo-16k",
            temperature=0.3,
        ),
        prompt=CONDENSE_QUESTION_PROMPT,
    ),
    combine_docs_chain=load_qa_chain(
        AzureChatOpenAI(
            **AzureOpenAIParams.us_east2,
            deployment_name="gpt-4-32k",
            temperature=0.5,
        ),
        chain_type="stuff",
        prompt=ChatPromptTemplate.from_messages(messages),
    ),
)
