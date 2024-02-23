import subprocess
import sys
from pathlib import Path


def get_base_path() -> Path:
    """Return repo base path"""
    return Path(__file__).parent.parent


def get_api_server_image() -> str:
    """Construct and return API image name"""
    version = "1.0.0-dev"
    image = f"ask-astro-{version}"
    return image


def generate_api_env_template():
    """
    Generate .env file in api/ with placeholder for env variable require to run API server

    See require variable: https://github.com/astronomer/ask-astro/blob/main/api/ask_astro/config.py
    """
    env_vars = {
        "FIRESTORE_INSTALLATION_STORE_COLLECTION": "< >",
        "FIRESTORE_STATE_STORE_COLLECTION": "< >",
        "FIRESTORE_MESSAGES_COLLECTION": "< >",
        "FIRESTORE_MENTIONS_COLLECTION": "< >",
        "FIRESTORE_ACTIONS_COLLECTION": "< >",
        "FIRESTORE_RESPONSES_COLLECTION": "< >",
        "FIRESTORE_REACTIONS_COLLECTION": "< >",
        "FIRESTORE_SHORTCUTS_COLLECTION": "< >",
        "FIRESTORE_TEAMS_COLLECTION": "",
        "FIRESTORE_REQUESTS_COLLECTION": "< >",
        "AZURE_OPENAI_USEAST_PARAMS": '{"openai_api_base": "https://< >.openai.azure.com/", "openai_api_key": "< >", '
        '"openai_api_type": "azure", "openai_api_version": "2023-05-15"}',
        "AZURE_OPENAI_USEAST2_PARAMS": '{"openai_api_base": "https://< >.openai.azure.com/", "openai_api_key": "< >", '
        '"openai_api_type": "azure", "openai_api_version": "2023-05-15"}',
        "ZENDESK_CREDENTIALS": '{"email": "< >", "password": "< >", "subdomain": "< >"}',
        "ZENDESK_ASSIGNEE_GROUP_ID": "160432390345",
        "SLACK_CLIENT_ID": "1394163359842.1394163359842",
        "SLACK_CLIENT_SECRET": "< >",
        "SLACK_SIGNING_SECRET": "< >",
        "LANGCHAIN_PROJECT": "< >",
        "LANGCHAIN_TRACING_V2": "< >",
        "LANGCHAIN_ENDPOINT": "< >",
        "LANGCHAIN_API_KEY": "< >",
        "OPENAI_API_KEY": "< >",
        "WEAVIATE_URL": "< >",
        "WEAVIATE_API_KEY": "< >",
        "WEAVIATE_INDEX_NAME": "< >",
        "WEAVIATE_TEXT_KEY": "< >",
        "GOOGLE_APPLICATION_CREDENTIALS": "./gcp.json",
    }

    env_path = f"{get_base_path()}/api/.env"
    with open(env_path, "w") as file:
        for key, value in env_vars.items():
            env = key + "=" + value + "\n"

            file.write(env)


def run_ui() -> None:
    """Start UI server on local machine"""
    ui_path = f"{get_base_path()}/ui"
    subprocess.check_call("npm install", shell=True, cwd=ui_path)
    subprocess.check_call("npm run dev", shell=True, cwd=ui_path)


def run_api_server() -> None:
    """Build backend API server and run it"""
    api_path = f"{get_base_path()}/api"
    subprocess.check_call(f"docker build . --tag {get_api_server_image()}", shell=True, cwd=api_path)
    subprocess.check_call(
        f"docker run --env-file ./.env -p 8080:8080 {get_api_server_image()}", shell=True, cwd=api_path
    )


def run_airflow() -> None:
    """Run airflow astro-cli project"""
    airflow_path = f"{get_base_path()}/airflow"
    subprocess.check_call("astro dev start", shell=True, cwd=airflow_path)


if __name__ == "__main__":
    commands = ["run-api-server", "run-ui", "run-airflow", "api-env-template"]
    if len(sys.argv) < 2:
        raise ValueError(f"Command is missing. Valid command is one of {commands}")
    command = sys.argv[1]
    if command == "run-api-server":
        run_api_server()
    elif command == "run-ui":
        run_ui()
    elif command == "run-airflow":
        run_airflow()
    elif command == "api-env-template":
        generate_api_env_template()
    else:
        raise ValueError(f"{command} is not valid. The valid commands is one of below:\n {commands}")
