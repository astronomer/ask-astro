"""
This module initializes a client instance from the LangSmith library.

The client provides functionalities related to the LangSmith package, and it's instantiated on module import.
"""
from langsmith import Client

# Singleton instance of the LangSmith Client.
langsmith_client = Client()
