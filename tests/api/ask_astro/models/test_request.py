from uuid import uuid4

from ask_astro.models.request import AskAstroRequest, HumanMessage, Source


def test_ask_astro_request_creation():
    """
    Test the creation of an instance of AskAstroRequest using valid inputs.
    Ensure that required attributes are correctly set after initialization.
    """
    request = AskAstroRequest(
        uuid=uuid4(),
        prompt="Test prompt",
        messages=[HumanMessage(content="Test message")],
        sources=[Source(name="Test Source", snippet="Test Snippet")],
        status="Test Status",
    )
    assert request is not None
    assert request.prompt == "Test prompt"
    assert request.status == "Test Status"


def test_to_firestore():
    """
    Test the conversion of an instance of AskAstroRequest to its Firestore dictionary representation.
    Ensure that the converted dictionary contains expected keys and values.
    """
    request = AskAstroRequest(
        uuid=uuid4(),
        prompt="Test prompt",
        messages=[HumanMessage(content="Test message")],
        sources=[Source(name="Test Source", snippet="Test Snippet")],
        status="Test Status",
    )
    firestore_dict = request.to_firestore()

    assert firestore_dict["prompt"] == "Test prompt"
    assert firestore_dict["status"] == "Test Status"
    assert len(firestore_dict["messages"]) == 1
    assert len(firestore_dict["sources"]) == 1


def test_from_dict():
    """
    Test the creation of an instance of AskAstroRequest from a dictionary representation.
    Ensure that the object is correctly initialized from the provided dictionary.
    """
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
