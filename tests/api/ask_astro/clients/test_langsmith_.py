from ask_astro.clients.langsmith_ import langsmith_client
from langsmith import Client


def test_langsmith_client_instance():
    """
    Test that the langsmith_client is an instance of the Client class from the langsmith library.
    """
    assert isinstance(
        langsmith_client, Client
    ), "langsmith_client is not an instance of Client from the langsmith library"
