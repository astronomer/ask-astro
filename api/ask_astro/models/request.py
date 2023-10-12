from datetime import datetime
from typing import Any
from uuid import UUID

from langchain.schema import AIMessage, BaseMessage, HumanMessage
from pydantic.v1 import BaseModel, Field


class Source(BaseModel):
    "Represents a source for a request."
    name: str = Field(..., description="The name of the source")
    snippet: str = Field(..., description="The snippet of the source")


class AskAstroRequest(BaseModel):
    "Represents a request to ask-astro."
    uuid: UUID = Field(..., description="The UUID of the request")
    prompt: str = Field(..., description="The prompt for the request")
    messages: list[BaseMessage] = Field(
        default_factory=list,
        description="The messages in the request",
    )
    sources: list[Source] = Field(
        default_factory=list,
        description="The sources for the request",
    )
    response: str | None = Field(
        None,
        description="The response to the request",
    )
    langchain_run_id: UUID | None = Field(
        None,
        description="The ID of the langchain run for the request",
    )
    score: int | None = Field(
        None,
        description="The score of the request",
    )
    status: str = Field(..., description="The status of the request")
    response_received_at: int | None = Field(
        None,
        description="The timestamp of when the response was received",
    )
    sent_at: int = Field(
        default_factory=lambda: int(datetime.now().timestamp()),
        description="The timestamp of the request",
    )
    is_processed: bool = Field(
        False,
        description="Whether the request has been processed",
    )
    is_example: bool = Field(
        False,
        description="Whether the request is an example",
    )

    def to_firestore(self) -> dict[str, Any]:
        """
        Returns the Request as a dict for Firestore.
        """
        return {
            "uuid": str(self.uuid),
            "prompt": self.prompt,
            "messages": [
                {
                    **message.dict(),
                    "type": message.type,
                }
                for message in self.messages
            ],
            "sources": [source.dict() for source in self.sources],
            "response": self.response,
            "status": self.status,
            "langchain_run_id": str(self.langchain_run_id),
            "score": self.score,
            "sent_at": self.sent_at,
            "response_received_at": self.response_received_at,
            "is_processed": self.is_processed,
            "is_example": self.is_example,
        }

    @classmethod
    def from_dict(cls, dict: dict[str, Any]) -> "AskAstroRequest":
        """
        Returns the Request from a dict.
        """
        return cls(
            uuid=UUID(dict["uuid"]),
            prompt=dict["prompt"],
            messages=[
                (HumanMessage if msg.get("type") == "human" else AIMessage)(
                    content=msg["content"],
                    additional_kwargs=msg.get("additional_kwargs", {}),
                )
                for msg in dict["messages"]
            ],
            sources=[Source(**source) for source in dict["sources"]],
            response=dict["response"],
            status=dict["status"],
            langchain_run_id=UUID(dict["langchain_run_id"]),
            score=dict["score"],
            sent_at=dict["sent_at"],
            response_received_at=dict.get("response_received_at"),
            is_processed=dict.get("is_processed", False),
            is_example=dict.get("is_example", False),
        )
