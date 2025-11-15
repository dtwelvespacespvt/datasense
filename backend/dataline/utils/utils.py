import base64
import logging
import random
import asyncio
from typing import AsyncGenerator

from fastapi import UploadFile
from sqlalchemy.exc import NoSuchModuleError, ProgrammingError

from dataline.errors import UserFacingError, ValidationError
from dataline.models.llm_flow.enums import QueryStreamingEventType

logger = logging.getLogger(__name__)


def get_sqlite_dsn_async(path: str) -> str:
    return f"sqlite+aiosqlite:///{path}"


def get_postgresql_dsn_async(path: str) -> str:
    return f"postgresql+asyncpg://{path}"


def get_mysql_dsn_async(path: str) -> str:
    return f"mysql+aiomysql://{path}"


def get_sqlite_dsn(path: str) -> str:
    return f"sqlite:///{path}"


def is_valid_sqlite_file(file: UploadFile) -> bool:
    # Check header for "SQLite format" by reading first few bytes
    # https://www.sqlite.org/fileformat2.html#the_database_header
    header = file.file.read(16)
    file.file.seek(0)  # Never forget to seek :D
    if header != b"SQLite format 3\000":
        return False
    return True


def generate_short_uuid() -> str:
    # Unique enough given the purpose of storing limited data files
    # Make sure only alphanumeric characters are used
    chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    sample = random.sample(chars, 8)
    return base64.b64encode("".join(sample).encode()).decode()[:8]


def stream_event_str(event: str, data: str) -> str:
    # https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format
    event_str = f"event: {event}"
    data_str = f"data: {data}"

    return f"{event_str}\n{data_str}\n\n"


async def generate_with_errors(generator: AsyncGenerator[str, None]) -> AsyncGenerator[str, None]:
    loop = asyncio.get_event_loop()
    last_ping_time = loop.time()

    gen_task = None
    timer_task = None

    try:
        gen_task = asyncio.create_task(anext(generator))

        while True:
            time_since_last_ping = loop.time() - last_ping_time
            time_to_next_ping = 5.0 - time_since_last_ping

            timer_task = asyncio.create_task(asyncio.sleep(max(0, time_to_next_ping)))

            done, pending = await asyncio.wait(
                {gen_task, timer_task},
                return_when=asyncio.FIRST_COMPLETED
            )

            if timer_task in done:
                yield stream_event_str(
                    QueryStreamingEventType.HEARTBEAT.value,
                    QueryStreamingEventType.HEARTBEAT.value
                )
                last_ping_time = loop.time()

            if gen_task in done:
                timer_task.cancel()

                try:
                    chunk = gen_task.result()
                    yield chunk
                    gen_task = asyncio.create_task(anext(generator))
                except StopAsyncIteration:
                    break

    except UserFacingError as e:
        logger.exception("Error in conversation query generator")
        yield stream_event_str(QueryStreamingEventType.ERROR.value, str(e))
    except Exception as e:
        logger.exception(f"Unexpected error in generator wrapper: {e}")
        yield stream_event_str(QueryStreamingEventType.ERROR.value, f"An unexpected error occurred: {e}")

    finally:
        if gen_task and not gen_task.done():
            gen_task.cancel()
        if timer_task and not timer_task.done():
            timer_task.cancel()


def forward_connection_errors(error: Exception) -> None:
    if isinstance(error, ProgrammingError):
        if "Must specify the full search path starting from database" in str(error):
            raise UserFacingError(
                (
                    "Invalid DSN. Please specify the full search path starting from the database"
                    "ex. 'SNOWFLAKE_SAMPLE_DATA/TPCH_SF1'"
                )
            )
    if isinstance(error, NoSuchModuleError):
        raise UserFacingError(f"Your version of DataLine does not support this database yet - {str(error)}")


def format_transcript(
        messages: list[dict[str, str]] | None = None,
    user_message: str | None = None,
    ai_message: str | None = None,
    sqls: list[str] | None = None,
) -> str:
    """
    Format a transcript string consistently for summarization.
    """
    transcript = ""

    if messages:
        for m in messages:
            role = m.get("role", "").capitalize()
            content = (m.get("content") or "").strip()
            if content:
                transcript += f"{role}: {content}\n"

    if user_message and ai_message:
        transcript += f"User: {user_message.strip()}\nAssistant: {ai_message.strip()}\n"

    if sqls:
        transcript += f"Generated SQL: {', '.join(sqls)}\n"

    return transcript.strip()

