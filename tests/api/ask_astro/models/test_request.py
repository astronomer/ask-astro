from uuid import uuid4

import pytest
from ask_astro.models.request import AskAstroRequest, HumanMessage, Source


# Define a fixture for AskAstroRequest
@pytest.fixture
def ask_astro_request_fixture():
    """
    Provides a fresh instance of AskAstroRequest with preset data
    for each test that needs it.
    """
    request = AskAstroRequest(
        uuid=uuid4(),
        prompt="Test prompt",
        messages=[HumanMessage(content="Test message")],
        sources=[Source(name="Test Source", snippet="Test Snippet")],
        status="Test Status",
    )
    return request


# Now use the fixture in your test functions
def test_ask_astro_request_creation(ask_astro_request_fixture):
    request = ask_astro_request_fixture
    assert request is not None
    assert request.prompt == "Test prompt"
    assert request.status == "Test Status"


def test_to_firestore(ask_astro_request_fixture):
    request = ask_astro_request_fixture
    firestore_dict = request.to_firestore()

    assert firestore_dict["prompt"] == "Test prompt"
    assert firestore_dict["status"] == "Test Status"
    assert len(firestore_dict["messages"]) == 1
    assert len(firestore_dict["sources"]) == 1


def test_from_dict():
    data = {
        "uuid": str(uuid4()),
        "prompt": "Test prompt",
        "messages": [{"content": "Test message", "type": "human"}],
        "sources": [{"name": "Test Source", "snippet": "Test Snippet"}],
        "status": "Test Status",
        "langchain_run_id": str(uuid4()),
        "score": 5,
        "sent_at": 123456789,
        "response": None,
    }
    request = AskAstroRequest.from_dict(data)

    assert request is not None
    assert request.prompt == "Test prompt"
    assert request.status == "Test Status"
    assert len(request.messages) == 1
    assert isinstance(request.messages[0], HumanMessage)
    assert len(request.sources) == 1
    assert isinstance(request.sources[0], Source)
