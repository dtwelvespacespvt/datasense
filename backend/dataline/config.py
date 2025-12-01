import sys
from pathlib import Path
import os

from pydantic_settings import BaseSettings

from dataline.utils.appdirs import user_data_dir

# https://pyinstaller.org/en/v6.6.0/runtime-information.html
IS_BUNDLED = bool(getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"))

USER_DATA_DIR = user_data_dir(appname="DataLine")

db_connection_string = os.environ.get("db_connection_string", "")
db_type = os.environ.get("db_type", "sqlite")

class EnvironmentType(str):
    development = "development"
    production = "production"


class Config(BaseSettings):
    # SQLite database will be mounted in the configuration directory
    # This is where all DataLine data is stored
    # Current dir / db.sqlite3
    sqlite_path: str = str(Path(USER_DATA_DIR) / "db.sqlite3")
    sqlite_echo: bool = False
    use_sqlite: bool = db_type == "sqlite"
    connection_string: str = db_connection_string
    type: str = db_type
    echo: bool = False

    # This is where all uploaded files are stored (ex. uploaded sqlite DBs)
    data_directory: str = str(Path(USER_DATA_DIR) / "data")

    sample_dvdrental_path: str = str(Path(__file__).parent.parent / "samples" / "dvd_rental.sqlite3")
    sample_netflix_path: str = str(Path(__file__).parent.parent / "samples" / "netflix.sqlite3")
    sample_titanic_path: str = str(Path(__file__).parent.parent / "samples" / "titanic.sqlite3")
    sample_spotify_path: str = str(Path(__file__).parent.parent / "samples" / "spotify.sqlite3")

    default_model: str = "gpt-5-mini"
    templates_path: Path = Path(__file__).parent.parent / "templates"
    assets_path: Path = Path(__file__).parent.parent / "assets"

    environment: str = EnvironmentType.development if not IS_BUNDLED else EnvironmentType.production
    release: str | None = None

    # HTTP Basic Authentication
    auth_username: str | None = None
    auth_password: str | None = None

    spa_mode: bool = False

    # CORS settings
    allowed_origins: str = (
        "http://localhost:7377,http://localhost:5173,http://0.0.0.0:7377,http://0.0.0.0:5173,http://127.0.0.1:7377,"
        "http://127.0.0.1:5173"  # comma separated list of origins
    )

    slack_url: str | None = None

    vector_db_url: str | None = None
    vector_db_type: str | None = None
    default_memory_conversation_depth: int = 3
    default_embedding_model: str = "text-embedding-3-small"
    vector_db_collection_glossary: str | None = "glossary"
    vector_db_collection_memory: str | None = "memory-v2"
    memory_analyzer_model: str = "gpt-4.1-nano"

    default_conversation_history_limit: int = 1
    default_sql_row_limit: int = 200
    JWT_SECRET: str | None= None
    JWT_ALGORITHM: str = "HS256"
    GOOGLE_CLIENT_ID: str | None = None
    ALLOWED_EMAIL_ORIGINS: list[str] = []

    # MailChimp Config
    MANDRILL_URL: str| None = "https://mandrillapp.com/api/1.0/messages/send"
    MANDRILL_API_KEY: str | None  = None
    BASE_MANDRILL_EMAIL: str | None = None


    def has_email_notification(self):
        return bool(self.MANDRILL_API_KEY)

    @property
    def has_auth(self) -> bool:
        return bool(self.auth_username and self.auth_password)

config = Config()
