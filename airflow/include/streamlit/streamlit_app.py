import json
import os
from pathlib import Path

import streamlit as st
from langchain.chains import RetrievalQA
from langchain.llms import OpenAI as langchain_openai
from langchain.vectorstores.weaviate import Weaviate as WeaviateStore
from PIL import Image
from weaviate_provider.hooks.weaviate import WeaviateHook

ask_astro_env = os.environ.get("ASK_ASTRO_ENV", "")

_WEAVIATE_CONN_ID = f"weaviate_{ask_astro_env}"

weaviate_hook = WeaviateHook(_WEAVIATE_CONN_ID)

st.set_page_config(layout="wide")

if "weaviate_client" not in st.session_state:
    weaviate_client = weaviate_hook.get_conn()
    st.session_state["weaviate_client"] = weaviate_client
else:
    weaviate_client = st.session_state["weaviate_client"]

if "openai_key" not in st.session_state:
    openai_key = json.loads(weaviate_hook.get_connection(_WEAVIATE_CONN_ID).extra)["X-OpenAI-Api-Key"]
    st.session_state["openai_key"] = openai_key
else:
    openai_key = st.session_state["openai_key"]

header_image = Image.open(Path(__file__).parent / "logo.png")
avatar_image = Path(__file__).parent.joinpath("logo1.png").as_posix()

st.markdown(
    """
<style>
.small-font {
    font-size:1px !important;
}
</style>""",
    unsafe_allow_html=True,
)
disclaimer = """<p><small>Disclaimer & Limitations\n\n Contextual Understanding: While Ask Astro excels at
understanding context to a significant extent, it may occasionally misinterpret complex or ambiguous queries.
In such cases, it’s recommended to provide additional clarifications or reach out to our human support team for
further assistance.</small></p>\n<p><small>Bias and Inaccuracy: Language models can be influenced by biases
present in the training data. Despite our efforts to mitigate biases, Ask Astro may inadvertently reflect biases
in its responses. We continuously monitor and address these issues, but user feedback is invaluable in helping us
improve.</small></p>\n<p><small>Non-Expert Advice: While Ask Astro is designed to provide accurate and helpful
information, it should not be considered a substitute for expert advice or professional consultation. In complex
or critical scenarios, it’s always recommended to consult our support team or relevant experts.</small></p>"""


def write_response(text: str):
    col1, mid, col2 = st.columns([1, 1, 20])
    with col1:
        st.image(avatar_image, width=60)
    with col2:
        st.write(text)
        # st.markdown(disclaimer, unsafe_allow_html=True)


with st.container():
    title_col, logo_col = st.columns([8, 2])
    with title_col:
        st.title("Welcome to Ask Astro!")
        st.write(
            """This Streamlit application is a simple troubleshooting app to test ingested documents.
                 After running the ingestion DAGs you can query the vector database with different interfaces as
                 seen in the tabs below."""
        )
    with logo_col:
        st.image(header_image)

weaviate_qna_tab, weaviate_gen_tab, langchain_weaviate_tab = st.tabs(
    ["Weaviate QNA Bot", "Weaviate Generative", "Langchain Weaviate Chat"]
)

with weaviate_qna_tab:
    st.header("Weaviate Q&A Module")
    st.write(
        """The [Question and Answer (Q&A) module in Weaviate]
        (https://weaviate.io/developers/weaviate/modules/reader-generator-modules/qna-openai) takes a question,
        creates vector embeddings for the question, performs a 'near_vector' search and returns the closest
        object."""
    )

    st.write("Example Questions:")
    st.write("- What is the AIRFLOW__CORE__PARALLELISM variable used for?")
    st.write("- When I set an environment variable via the Astro UI, does my deployment restart?")
    st.write("- How can I use dynamic tasks?")

    question = st.text_area("Question for Q&A", placeholder="")

    if question:
        st.write("Showing QNA search results for:  " + question)

        st.header("Answer")
        ask = {
            "question": question,
            "properties": ["content", "header", "docSource"],
            # "certainty": 0.0
        }

        st.subheader("Docs results")
        results = (
            weaviate_client.query.get("Docs", ["docLink", "_additional {answer {hasAnswer property result} }"])
            .with_ask(ask)
            .with_limit(3)
            .with_additional(["certainty", "id", "distance"])
            .do()
        )

        if results.get("errors"):
            for error in results["errors"]:
                if ("no api key found" or "remote client vectorize: failed with status: 401 error") in error["message"]:
                    raise Exception("Cannot vectorize.  Check the OPENAI_API key as environment variable.")
                else:
                    st.write(error["message"])
        elif len(results["data"]["Get"]["Docs"]) > 0:
            docLinks = []
            link_count = 1
            for result in results["data"]["Get"]["Docs"]:
                if result["_additional"]["answer"]["hasAnswer"]:
                    write_response(result["_additional"]["answer"]["result"])
                docLinks.append(f"[{link_count}]({result['docLink']})")
                link_count = link_count + 1
            st.write(",".join(docLinks))
            st.markdown(disclaimer, unsafe_allow_html=True)

with weaviate_gen_tab:
    st.header("Weaviate Generative Module")
    st.write(
        """The [Generative Search module in Weaviate](https://weaviate.io/developers/weaviate/modules/reader-generator-modules/generative-openai)
             generates responses for a question.  Weaviate vectorizes the question, and performs a 'near_vector'
             search and returns the closest 3 objects.   These objects are provided as context to gpt-4. Enter a
             question to see the prompt."""
    )

    prompt_path = Path(__file__).parent.joinpath("combine_docs_sys_prompt_webapp.txt")
    prompt = prompt_path.read_text()

    st.write(f"The prompt text can be found in the file __`{prompt_path}`__")

    st.write("Example Questions:")
    st.write("- What is the AIRFLOW__CORE__PARALLELISM variable used for?")
    st.write("- When I set an environment variable via the Astro UI, does my deployment restart?")
    st.write("- How can I use dynamic tasks?")

    question = st.text_area("Question for generative search", placeholder="")

    if question:
        st.header("Generated Answer")

        results = (
            weaviate_client.query.get("Docs", ["docLink"])
            .with_near_text({"concepts": question})
            .with_generate(grouped_task=prompt.format(question=question))
            .with_limit(3)
            .do()
        )

        if results.get("errors"):
            for error in results["errors"]:
                if ("no api key found" or "remote client vectorize: failed with status: 401 error") in error["message"]:
                    raise Exception("Cannot vectorize.  Check the OPENAI_API key as environment variable.")
                else:
                    st.write(error["message"])
        elif len(results["data"]["Get"]["Docs"]) > 0:
            docLinks = []
            link_count = 1
            for result in results["data"]["Get"]["Docs"]:
                if result["_additional"]["generate"] and not result["_additional"]["generate"]["error"]:
                    write_response(result["_additional"]["generate"]["groupedResult"] + "\n")
                docLinks.append(f"[{link_count}]({result['docLink']})")
                link_count = link_count + 1
            st.write(",".join(docLinks))
            st.subheader("Prompt")
            st.write("This answer was generated with the following prompt: \n\n")
            st.write(prompt.replace("-", ""))
            st.markdown(disclaimer, unsafe_allow_html=True)

with langchain_weaviate_tab:
    st.header("Langchain Q&A with Weaviate")
    st.write(
        """[LangChain](https://python.langchain.com/docs/use_cases/question_answering/) provides many libraries
             for retrieval augmented generation. The example here performs a simple context-based Q&A which should
             be similar to the Weaviate Q&A.  Much more complex prompts can be generated with chain-of-thought, memory
             or other techniques.
             """
    )

    st.write("Example Questions:")
    st.write("- What is the AIRFLOW__CORE__PARALLELISM variable used for?")
    st.write("- When I set an environment variable via the Astro UI, does my deployment restart?")
    st.write("- How can I use dynamic tasks?")

    vectorstore = WeaviateStore(weaviate_client, "Docs", "content")
    openai_client = langchain_openai(temperature=0.2, openai_api_key=openai_key)
    qa = RetrievalQA.from_chain_type(llm=openai_client, chain_type="map_reduce", retriever=vectorstore.as_retriever())

    st.subheader("Please enter a question or dialogue to get started!")

    langchain_question = st.text_area("Langchain Prompt", placeholder="")

    if langchain_question:
        result = qa.run(langchain_question)

        write_response(result)
        st.markdown(disclaimer, unsafe_allow_html=True)
