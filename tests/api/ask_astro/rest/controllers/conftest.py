import pytest
from sanic import Sanic
from sanic_testing import TestManager


@pytest.fixture
def app() -> Sanic:
    """Fixture to create a new Sanic application for testing."""
    sanitized_name = __name__.replace(".", "_")
    app_instance = Sanic(sanitized_name)
    TestManager(app_instance)

    from ask_astro.rest.controllers import register_routes

    register_routes(app_instance)

    return app_instance
