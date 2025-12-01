import json
import logging
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import BinaryIO, Annotated
from uuid import UUID

import pandas as pd
import pyreadstat
from fastapi import Depends, UploadFile
from openai import APIError
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from typing import Any

from dataline.auth import AuthManager, get_auth_manager

from dataline.auth import UserInfo
from dataline.config import config
from dataline.errors import ValidationError
from dataline.models.connection.model import ConnectionModel
from dataline.models.connection.schema import (
    ConnectionSchemaTable,
    ConnectionOptions,
    ConnectionOut,
    ConnectionSchema,
    ConnectionUpdateIn,
    ConnectionSchemaTableColumn,
    ConnectionSchemaTableColumnRelationship, RelationshipOut,
)
from dataline.repositories.base import AsyncSession, NotFoundError, NotUniqueError
from dataline.repositories.connection import (
    ConnectionCreate,
    ConnectionRepository,
    ConnectionType,
    ConnectionUpdate,
)
from dataline.repositories.user import UserRepository
from dataline.services.file_parsers.excel_parser import ExcelParserService
from dataline.services.llm_flow.llm_calls.database_description_generator import database_description_generator_prompt
from dataline.services.llm_flow.utils import DatalineSQLDatabase as SQLDatabase
from dataline.services.settings import SettingsService
from dataline.utils.memory import PersistentChatMemory
from dataline.utils.utils import (
    forward_connection_errors,
    generate_short_uuid,
    get_sqlite_dsn,
)
from collections import defaultdict
import re
from sqlalchemy import text

logger = logging.getLogger(__name__)


def fetch_table_schemas(options: ConnectionOptions):
    table_schemas = defaultdict(list)
    for schema in options.schemas:
        if schema.enabled:
            for table in schema.tables:
                if table.enabled:
                    for column in table.columns:
                        if column.enabled:
                            table_schemas[f"{schema.name}.{table.name}"].append({
                                "name": column.name,
                                "type": column.type,
                                "primary_key": column.primary_key
                            })

    return table_schemas


def normalize(col_name):
    return re.sub(r'[^a-z0-9]', '', col_name.lower())


def is_potential_fk(from_col, to_table, to_col, synonyms):
    from_name = normalize(from_col)
    to_name = normalize(to_col["name"])
    to_table_norm = normalize(to_table)

    # Pattern matching
    likely_match = (
            from_name == to_name or
            from_name == f"{to_table_norm}{to_name}" or
            any(from_name == f"{to_table_norm}{to_name}{syn}".lower() for syn in synonyms) or
            any(from_name == f"{to_table_norm}{syn}".lower() for syn in synonyms) or
            any(from_name.endswith(syn.lower()) and to_name in [s.lower() for s in synonyms] for syn in synonyms) or
            any(to_name.endswith(syn.lower()) and from_name in [s.lower() for s in synonyms] for syn in synonyms) or
            (from_name.replace(to_table_norm, '') in [s.lower() for s in synonyms] and to_name in [s.lower() for s in synonyms]) or
            any(from_name.endswith(syn.lower()) and any(to_name.endswith(s.lower()) for s in synonyms) for syn in synonyms) or
            any(to_name.endswith(syn.lower()) and any(from_name.endswith(s.lower()) for s in synonyms) for syn in synonyms)
    )

    return likely_match


async def validate_fk_by_value_overlap(from_table, from_column, to_table, to_column, db,
                                       from_type: str, to_type: str
                                       , ignore_columns_in_relationship: list[str]
                                       , ignore_types_in_relationship: list[str]
                                       , ignore_comparisons_in_relationship: list[str]
                                       , ignore_prefix_in_relationship: list[str]):
    def normalize_sql_type(t):
        return t.lower().split('(')[0].strip()

    from_base = normalize_sql_type(from_type)
    to_base = normalize_sql_type(to_type)
    if from_column in ignore_columns_in_relationship:
        return 0.0
    if from_base in ignore_types_in_relationship:
        return 0.0
    if from_column == to_column and from_column in ignore_comparisons_in_relationship:
        return 0.0
    if any(from_column.startswith(_value) for _value in ignore_prefix_in_relationship):
        return 0.0
    if from_base != to_base:
        logger.info(f"[INFO] Casting {to_table}.{to_column} ({to_base}) → {from_base} to match")
        return 0.0
    else:
        casted_to_column = to_column

    query = text(f"""
        SELECT COUNT(*) AS total_matches
        FROM {from_table}
        WHERE {from_column} IN (SELECT DISTINCT {casted_to_column} FROM {to_table})
    """)

    total_query = text(f"SELECT COUNT(*) AS total FROM {from_table}")

    try:
        with db._engine.connect() as conn:
            matches = conn.execute(query).scalar() or 0
            if matches == 0:
                return 0.0
            total = conn.execute(total_query).scalar() or 1  # avoid div by zero
            overlap = matches / total
            logger.info(f"[INFO] {from_table}.{from_column} → {to_table}.{to_column} ({to_base}) -> {overlap}")
            return overlap
    except Exception as e:
        logger.exception(f"Overlap validation failed for {from_table}.{from_column} -> {to_table}.{to_column}: {e}")
        return 0.0

async def get_distinct_values(schema, table, column, db):
    query = text(f"SELECT DISTINCT {column} FROM {schema}.{table}")

    try:
        with db._engine.connect() as conn:
            result = conn.execute(query)
            values = [row[0] for row in result.fetchall()]
            flat = await extract_flat_string_list([v for v in values if v is not None])
            flat = [s for s in flat if s.strip()]
            unique_flat = list(dict.fromkeys(flat))
            return unique_flat
    except Exception as e:
        logger.exception(f"Failed to get distinct values from {table}.{column}: {e}")
        return []

async def extract_flat_string_list(nested: list[Any]) -> list[str]:
    flat = []
    for item in nested:
        if isinstance(item, list):
            for sub in item:
                if isinstance(sub, str):
                    flat.append(sub)
        elif isinstance(item, str):
            flat.append(item)

    return flat

async def infer_relationships_per_column(schema: str, table: str, column: str, column_type: str, table_schemas, synonyms, db
                                         , existing_relationship: list[ConnectionSchemaTableColumnRelationship]
                                         , ignore_columns_in_relationship: list[str]
                                         , ignore_types_in_relationship: list[str]
                                         , ignore_comparisons_in_relationship: list[str]
                                         , ignore_prefix_in_relationship: list[str]
                                         , threshold=0.7) -> list[RelationshipOut]:
    relationships = []
    for to_table, to_cols in table_schemas.items():
        if f"{schema}.{table}" == to_table:
            continue
        for to_col in to_cols:
            if existing_relationship is not None and len(existing_relationship) > 0:
                for relationship in existing_relationship:
                    if relationship.schema_name == to_table.split(".")[0] and relationship.table == to_table.split(".")[1] and relationship.column == to_col["name"]:
                        relationships.append(RelationshipOut(
                            schema_name=relationship.schema_name,
                            table=relationship.table,
                            column=relationship.column,
                            enabled=relationship.enabled,
                        ))
                        continue
            if is_potential_fk(column, to_table.split(".")[1], to_col, synonyms):
                overlap = await validate_fk_by_value_overlap(f"{schema}.{table}", column, to_table, to_col["name"], db, column_type, to_col["type"]
                                                             , ignore_columns_in_relationship
                                                             , ignore_types_in_relationship
                                                             , ignore_comparisons_in_relationship
                                                             , ignore_prefix_in_relationship)
                if overlap >= threshold:
                    relationships.append(RelationshipOut(
                        schema_name=to_table.split(".")[0],
                        table=to_table.split(".")[1],
                        column=to_col["name"],
                        enabled=True,
                    ))
    return relationships


async def infer_relationships(options: ConnectionOptions, table_schemas, synonyms, db
                              , ignore_columns_in_relationship: list[str]
                              , ignore_types_in_relationship: list[str]
                              , ignore_comparisons_in_relationship: list[str]
                              , ignore_prefix_in_relationship: list[str]
                              , threshold=0.7):
    for schema in options.schemas:
        if schema.enabled:
            for from_table in schema.tables:
                if from_table.enabled:
                    for from_col in from_table.columns:
                        if from_col.enabled:
                            relationships = await infer_relationships_per_column(schema.name, from_table.name, from_col.name, from_col.type, table_schemas, synonyms, db, from_col.relationship, ignore_columns_in_relationship, ignore_types_in_relationship, ignore_comparisons_in_relationship, ignore_prefix_in_relationship, threshold)
                            if len(relationships) > 0:
                                from_col.relationship = [ConnectionSchemaTableColumnRelationship(
                                        schema_name=relationship.schema_name,
                                        table=relationship.table,
                                        column=relationship.column,
                                        enabled=relationship.enabled,
                                    )
                                    for relationship in relationships]
    return ConnectionOptions(schemas=options.schemas)


class ConnectionService:
    connection_repo: ConnectionRepository
    settings_service: SettingsService
    user_info: UserInfo
    user_repo: UserRepository

    def __init__(self, auth_manager: Annotated[AuthManager, Depends(get_auth_manager)], connection_repo: ConnectionRepository = Depends(ConnectionRepository),
                 settings_service: SettingsService = Depends(SettingsService), user_repo:UserRepository = Depends(UserRepository)) -> None:
        self.connection_repo = connection_repo
        self.settings_service = settings_service
        self.user_repo = user_repo
        self.auth_manager = auth_manager

    async def get_connection(self, session: AsyncSession, connection_id: UUID) -> ConnectionOut:
        connection = await self.connection_repo.get_by_uuid(session, connection_id)
        return ConnectionOut.model_validate(connection)

    async def get_connection_from_dsn(self, session: AsyncSession, dsn: str) -> ConnectionOut:
        connection = await self.connection_repo.get_by_dsn(session, dsn=dsn)
        return ConnectionOut.model_validate(connection)

    async def get_connections(self, session: AsyncSession) -> list[ConnectionOut]:
        connections = await self.connection_repo.list_all(session)
        return [ConnectionOut.model_validate(connection) for connection in connections]

    async def get_connection_by_uuid(self, session:AsyncSession, connection_uuid: UUID):
        connection = await self.connection_repo.get_by_uuid(session, connection_uuid)
        return ConnectionOut.model_validate(connection)

    async def delete_connection(self, session: AsyncSession, connection_id: UUID) -> None:
        await self.connection_repo.delete_by_uuid(session, connection_id)

    async def get_connections_by_user_uuid(self, session:AsyncSession):
        if self.auth_manager.is_admin():
            return await self.connection_repo.list_all(session)
        user = await self.user_repo.get_by_uuid(session, await self.auth_manager.get_user_id())
        if user.config and user.config.get('connections'):
            return await self.connection_repo.get_all_by_uuids(session, user.config.get('connections',[]))
        return []

    async def get_db_from_dsn(self, dsn: str) -> SQLDatabase:
        # Check if connection can be established before saving it
        try:
            db = SQLDatabase.from_uri(dsn)
            database = db._engine.url.database

            if not database:
                raise ValidationError("Invalid DSN. Database name is missing, append '/DBNAME'.")

            return db

        except OperationalError as exc:
            # Try again replacing localhost with host.docker.internal to connect with DBs running in docker
            if "localhost" in dsn:
                dsn = dsn.replace("localhost", "host.docker.internal")
                try:
                    db = SQLDatabase.from_uri(dsn)
                    database = db._engine.url.database

                    if not database:
                        raise ValidationError("Invalid DSN. Database name is missing, append '/DBNAME'.")

                    return db
                except OperationalError as e:
                    logger.exception(e)
                    raise ValidationError("Failed to connect to database, please check your DSN.")
                except Exception as e:
                    forward_connection_errors(e)

            logger.exception(exc)
            raise ValidationError("Failed to connect to database, please check your DSN.")

        except Exception as e:
            forward_connection_errors(e)
            logger.exception(e)
            raise ValidationError("Failed to connect to database, please check your DSN.")

    async def check_dsn_already_exists(self, session: AsyncSession, dsn: str) -> None:
        try:
            existing_connection = await self.connection_repo.get_by_dsn(session, dsn=dsn)
            if existing_connection:
                raise NotUniqueError("Connection already exists.")
        except NotFoundError:
            pass

    async def check_dsn_already_exists_or_none(self, session: AsyncSession, dsn: str) -> ConnectionModel | None:
        try:
            return await self.connection_repo.get_by_dsn(session, dsn=dsn)
        except NotFoundError:
            return None

    async def _build_connection_schema_table(self, session: AsyncSession, schema: str, table: str, db,
                                             generate_columns: bool, generate_descriptions: bool) \
            -> ConnectionSchemaTable:
        columns = db.get_column_info_per_table_per_schema(schema, table) if generate_columns else []
        table_description, column_descriptions = await self.enrich_table_with_llm(session, table, columns) \
            if generate_descriptions and len(columns) > 0 else ("", {})

        return ConnectionSchemaTable(
            name=table,
            enabled=True,
            description=table_description,
            columns=[
                ConnectionSchemaTableColumn(
                    name=col["name"],
                    type=col["type"],
                    primary_key=col["primary_key"],
                    enabled=True,
                    description=column_descriptions.get(col["name"], ""),
                    reverse_look_up=False
                )
                for col in columns
            ] if len(columns) > 0 else []
        )

    async def _build_connection_schema_table_from_existing(self, session: AsyncSession, schema: str, table: str, db,
                                                           generate_columns: bool, generate_descriptions: bool,
                                                           connection_schema_table: ConnectionSchemaTable) \
            -> ConnectionSchemaTable:
        if connection_schema_table is None or len(connection_schema_table.columns) == 0:
            columns = db.get_column_info_per_table_per_schema(schema, table) if generate_columns else []
            table_description, column_descriptions = await self.enrich_table_with_llm(session, table, columns) \
                if generate_descriptions and len(columns) > 0 else ("", {})
            if connection_schema_table is None:
                enabled = True
            else:
                enabled = connection_schema_table.enabled
            return ConnectionSchemaTable(
                name=table,
                enabled=enabled,
                description=table_description,
                columns=[
                    ConnectionSchemaTableColumn(
                        name=col["name"],
                        type=col["type"],
                        primary_key=col["primary_key"],
                        enabled=True,
                        description=column_descriptions.get(col["name"], ""),
                        reverse_look_up=False
                    )
                    for col in columns
                ] if len(columns) > 0 else []
            )
        else:
            columns = connection_schema_table.columns
            table_description = connection_schema_table.description
            if generate_descriptions and (table_description is None or str(table_description).strip() == ""):
                columns_dict_list = [col.dict() for col in columns]
                table_description, column_descriptions = await self.enrich_table_with_llm(session, table,
                                                                                          columns_dict_list)
            else:
                column_descriptions = {col.name: col.description for col in columns}
            column_enabled = {col.name: col.enabled for col in columns}
            return ConnectionSchemaTable(
                name=connection_schema_table.name,
                enabled=connection_schema_table.enabled,
                description=table_description,
                columns=[
                    ConnectionSchemaTableColumn(
                        name=col.name,
                        type=col.type,
                        primary_key=col.primary_key,
                        enabled=column_enabled.get(col.name, False),
                        description=column_descriptions.get(col.name, ""),
                        relationship=col.relationship,
                        possible_values=col.possible_values,
                        reverse_look_up = col.reverse_look_up
                    )
                    for col in columns
                ] if len(columns) > 0 else []
            )

    async def enrich_table_with_llm(self, session: AsyncSession, table: str, columns: list[dict]) -> tuple[str, dict]:
        from openai import OpenAI
        user_details = await self.settings_service.get_model_details(session)
        api_key = user_details.openai_api_key.get_secret_value()
        base_url = user_details.openai_base_url
        try:
            client = OpenAI(api_key=api_key, base_url=base_url)
            prompt = database_description_generator_prompt(table, columns)
            description_generator_response = client.chat.completions.create(
                model=config.memory_analyzer_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=1
            )
            parsed = json.loads(description_generator_response.choices[0].message.content)
            return parsed["tableDescription"], parsed["columns"]
        except APIError as e:
            logger.exception(f"[LLM] Failed to describe {table}: {e}")
            return "", {}

    async def update_connection(
            self, session: AsyncSession, connection_uuid: UUID, data: ConnectionUpdateIn
    ) -> ConnectionOut:
        update = ConnectionUpdate()
        if data.dsn:
            # Check if connection already exists and is different from the current one
            existing_connection = await self.check_dsn_already_exists_or_none(session, data.dsn)
            if existing_connection is not None and existing_connection.id != connection_uuid:
                raise NotUniqueError("Connection DSN already exists.")

            # Check if connection can be established before saving it
            db = await self.get_db_from_dsn(data.dsn)
            url = make_url(data.dsn)
            query = url.query
            generate_columns = query.get("generate_columns", "false").lower() == "true"
            generate_descriptions = query.get("generate_descriptions", "false").lower() == "true"
            update.dsn = data.dsn
            update.database = db._engine.url.database
            update.dialect = db.dialect
            current_connection = await self.get_connection(session, connection_uuid)
            old_options = (
                ConnectionOptions.model_validate(current_connection.options) if current_connection.options else None
            )
            update.options = await self.merge_options(session, old_options, db, generate_columns, generate_descriptions)
            update.unique_value_dict = await self.generate_unique_value_dict(update.options)
        elif data.options:
            # only modify options if dsn hasn't changed
            update.options = data.options
            # generate Unique Value Dict
            update.unique_value_dict = await self.generate_unique_value_dict(update.options)
        if data.name:
            update.name = data.name
        if data.glossary:
            update.glossary = data.glossary
        if data.config:
            update.config = data.config
        updated_connection = await self.connection_repo.update_by_uuid(session, connection_uuid, update)
        return ConnectionOut.model_validate(updated_connection)

    async def generate_descriptions(
            self, session: AsyncSession, connection_uuid: UUID, data: ConnectionUpdateIn
    ) -> ConnectionOut:
        update = ConnectionUpdate()
        # Check if connection already exists and is different from the current one
        existing_connection = await self.check_dsn_already_exists_or_none(session, data.dsn)
        if existing_connection is not None and existing_connection.id != connection_uuid:
            raise NotUniqueError("Connection DSN already exists.")

        # Check if connection can be established before saving it
        db = await self.get_db_from_dsn(data.dsn)
        url = make_url(data.dsn)
        query = url.query
        generate_columns = query.get("generate_columns", "false").lower() == "true"
        if not generate_columns:
            raise ValidationError("Please include generate_columns query param in the dsn")
        generate_descriptions = True
        current_connection = await self.get_connection(session, connection_uuid)
        old_options = (
            ConnectionOptions.model_validate(current_connection.options) if current_connection.options else None
        )
        update.options = await self.merge_options(session, old_options, db, generate_columns, generate_descriptions)
        update.unique_value_dict = await self.generate_unique_value_dict(update.options)
        updated_connection = await self.connection_repo.update_by_uuid(session, connection_uuid, update)
        return ConnectionOut.model_validate(updated_connection)

    async def generate_relationships_per_column(self, session: AsyncSession, connection_uuid: UUID, schema: str, table: str, column: str,
                                                column_type: str) -> list[RelationshipOut]:
        current_connection = await self.get_connection(session, connection_uuid)
        # Check if connection can be established before saving it
        db = await self.get_db_from_dsn(current_connection.dsn)
        url = make_url(current_connection.dsn)
        query = url.query
        fk_synonyms = query.get("fk_synonyms")
        synonyms = [t.strip() for t in fk_synonyms.split(",")] if fk_synonyms else []
        if len(synonyms) == 0:
            raise ValidationError("foreign key synonyms are not defined in dsn")
        str_ignore_columns_in_relationship = query.get("ignore_columns_in_relationship")
        ignore_columns_in_relationship = [t.strip() for t in str_ignore_columns_in_relationship.split(",")] if str_ignore_columns_in_relationship else []
        str_ignore_types_in_relationship = query.get("ignore_types_in_relationship")
        ignore_types_in_relationship = [t.strip() for t in str_ignore_types_in_relationship.split(",")] if str_ignore_types_in_relationship else []
        str_ignore_comparisons_in_relationship = query.get("ignore_comparisons_in_relationship")
        ignore_comparisons_in_relationship = [t.strip() for t in str_ignore_comparisons_in_relationship.split(",")] if str_ignore_comparisons_in_relationship else []
        str_ignore_prefix_in_relationship = query.get("ignore_prefix_in_relationship")
        ignore_prefix_in_relationship = [t.strip() for t in str_ignore_prefix_in_relationship.split(",")] if str_ignore_prefix_in_relationship else []
        old_options = (
            ConnectionOptions.model_validate(current_connection.options) if current_connection.options else None
        )
        return await infer_relationships_per_column(schema, table, column, column_type, fetch_table_schemas(options=old_options), synonyms=synonyms, db=db, existing_relationship=[], ignore_columns_in_relationship=ignore_columns_in_relationship, ignore_types_in_relationship=ignore_types_in_relationship, ignore_comparisons_in_relationship=ignore_comparisons_in_relationship, ignore_prefix_in_relationship=ignore_prefix_in_relationship, threshold=0.05)

    async def get_possible_values_per_column(self, session: AsyncSession, connection_uuid: UUID, schema: str, table: str, column: str
                                             ) -> list:
        current_connection = await self.get_connection(session, connection_uuid)
        db = await self.get_db_from_dsn(current_connection.dsn)
        return await get_distinct_values(schema, table, column, db=db)

    async def generate_relationships(
            self, session: AsyncSession, connection_uuid: UUID, data: ConnectionUpdateIn
    ) -> ConnectionOut:
        update = ConnectionUpdate()
        # Check if connection already exists and is different from the current one
        existing_connection = await self.check_dsn_already_exists_or_none(session, data.dsn)
        if existing_connection is not None and existing_connection.id != connection_uuid:
            raise NotUniqueError("Connection DSN already exists.")

        # Check if connection can be established before saving it
        db = await self.get_db_from_dsn(data.dsn)
        current_connection = await self.get_connection(session, connection_uuid)
        old_options = (
            ConnectionOptions.model_validate(current_connection.options) if current_connection.options else None
        )
        url = make_url(data.dsn)
        query = url.query
        fk_synonyms = query.get("fk_synonyms")
        synonyms = [t.strip() for t in fk_synonyms.split(",")] if fk_synonyms else []
        if len(synonyms) == 0:
            raise ValidationError("foreign key synonyms are not defined in dsn")
        str_ignore_columns_in_relationship = query.get("ignore_columns_in_relationship")
        ignore_columns_in_relationship = [t.strip() for t in str_ignore_columns_in_relationship.split(",")] if str_ignore_columns_in_relationship else []
        str_ignore_types_in_relationship = query.get("ignore_types_in_relationship")
        ignore_types_in_relationship = [t.strip() for t in str_ignore_types_in_relationship.split(",")] if str_ignore_types_in_relationship else []
        str_ignore_comparisons_in_relationship = query.get("ignore_comparisons_in_relationship")
        ignore_comparisons_in_relationship = [t.strip() for t in str_ignore_comparisons_in_relationship.split(",")] if str_ignore_comparisons_in_relationship else []
        str_ignore_prefix_in_relationship = query.get("ignore_prefix_in_relationship")
        ignore_prefix_in_relationship = [t.strip() for t in str_ignore_prefix_in_relationship.split(",")] if str_ignore_prefix_in_relationship else []
        update.options = await infer_relationships(old_options, fetch_table_schemas(options=old_options), synonyms=synonyms, db=db, ignore_columns_in_relationship=ignore_columns_in_relationship, ignore_types_in_relationship=ignore_types_in_relationship, ignore_comparisons_in_relationship=ignore_comparisons_in_relationship, ignore_prefix_in_relationship=ignore_prefix_in_relationship, threshold=0.05)
        updated_connection = await self.connection_repo.update_by_uuid(session, connection_uuid, update)
        return ConnectionOut.model_validate(updated_connection)

    async def create_connection(
            self,
            session: AsyncSession,
            dsn: str,
            name: str,
            connection_type: str | None = None,
            is_sample: bool = False,
    ) -> ConnectionOut:
        # Check if connection can be established before saving it
        db = await self.get_db_from_dsn(dsn)
        if not connection_type:
            connection_type = db.dialect

        url = make_url(dsn)
        query = url.query
        generate_columns = query.get("generate_columns", "false").lower() == "true"
        generate_descriptions = query.get("generate_descriptions", "false").lower() == "true"
        # Check if connection already exists
        await self.check_dsn_already_exists(session, dsn)
        connection_schemas: list[ConnectionSchema] = [
            ConnectionSchema(
                name=schema,
                tables=[await self._build_connection_schema_table(session, schema, table, db, generate_columns
                                                            , generate_descriptions) for table in tables],
                enabled=True,
            )
            for schema, tables in db._all_tables_per_schema.items()
        ]
        connection = await self.connection_repo.create(
            session,
            ConnectionCreate(
                dsn=dsn,
                database=db._engine.url.database,
                name=name,
                dialect=db.dialect,
                type=connection_type,
                is_sample=is_sample,
                options=ConnectionOptions(schemas=connection_schemas),
            ),
        )
        return ConnectionOut.model_validate(connection)

    async def create_sqlite_connection(
        self, session: AsyncSession, file: BinaryIO, name: str, is_sample: bool = False
    ) -> ConnectionOut:
        generated_name = generate_short_uuid() + ".sqlite"
        file_path = Path(config.data_directory) / generated_name
        with file_path.open("wb") as f:
            f.write(file.read())

        # Create connection with the locally copied file
        dsn = get_sqlite_dsn(str(file_path.absolute()))
        return await self.create_connection(session, dsn=dsn, name=name, is_sample=is_sample)

    async def create_csv_connection(self, session: AsyncSession, file: UploadFile, name: str) -> ConnectionOut:
        generated_name = generate_short_uuid() + ".sqlite"
        file_path = Path(config.data_directory) / generated_name

        # Connect to the SQLite database (it will be created if it doesn't exist)
        conn = sqlite3.connect(file_path)
        # Load CSV file into a Pandas dataframe directly from the URL
        data_df = pd.read_csv(file.file)
        # Write the dataframe to the SQLite database
        table_name = name.lower().replace(" ", "_")
        data_df.to_sql(table_name, conn, if_exists="replace", index=False)
        # Commit and close connection to new SQLite database
        conn.commit()
        conn.close()

        # Create connection with the locally copied file
        dsn = get_sqlite_dsn(str(file_path.absolute()))
        return await self.create_connection(
            session, dsn=dsn, name=name, connection_type=ConnectionType.csv.value, is_sample=False
        )

    async def create_excel_connection(self, session: AsyncSession, file: UploadFile, name: str) -> ConnectionOut:
        generated_name = generate_short_uuid() + ".sqlite"
        file_path = Path(config.data_directory) / generated_name

        # Connect to the SQLite database and input data (it will be created if it doesn't exist)
        conn = sqlite3.connect(file_path)
        ExcelParserService.to_sqlite_offline_secure(file.file, conn, name)
        conn.commit()
        conn.close()

        # Create connection with the locally copied file
        dsn = get_sqlite_dsn(str(file_path.absolute()))
        return await self.create_connection(
            session, dsn=dsn, name=name, connection_type=ConnectionType.excel.value, is_sample=False
        )

    async def create_sas7bdat_connection(self, session: AsyncSession, file: UploadFile, name: str) -> ConnectionOut:
        generated_name = generate_short_uuid() + ".sqlite"
        file_path = Path(config.data_directory) / generated_name

        # Connect to the SQLite database (it will be created if it doesn't exist)
        conn = sqlite3.connect(file_path)

        # Create a temporary file to store the uploaded content
        with tempfile.NamedTemporaryFile(delete=False, suffix=".sas7bdat") as temp_file:
            temp_file.write(await file.read())
            temp_file_path = temp_file.name

        try:
            # Load sas7bdat file into a Pandas dataframe from the temporary file
            data_df, meta = pyreadstat.read_sas7bdat(temp_file_path)

            new_column_names = {}

            # Loop through the column names and their labels
            for col, label in meta.column_names_to_labels.items():
                if label:
                    # If a label exists, use it as the new column name
                    new_column_names[col] = label
                else:
                    # If no label exists, keep the original column name
                    new_column_names[col] = col
            # Rename the columns in the DataFrame
            data_df.rename(columns=new_column_names, inplace=True)

            # Write the dataframe to the SQLite database
            table_name = name.lower().replace(" ", "_")
            data_df.to_sql(table_name, conn, if_exists="replace", index=False)

            # Commit and close connection to new SQLite database
            conn.commit()
            conn.close()

            # Create connection with the locally copied file
            dsn = get_sqlite_dsn(str(file_path.absolute()))
            return await self.create_connection(
                session, dsn=dsn, name=name, connection_type=ConnectionType.sas.value, is_sample=False
            )
        finally:
            # Clean up the temporary file
            os.unlink(temp_file_path)

    async def merge_options(self, session: AsyncSession, old_options: ConnectionOptions | None, db: SQLDatabase,
                            generate_columns: bool, generate_descriptions: bool) -> ConnectionOptions:
        if old_options is None:
            # No options in the db, create new ConnectionOptions with everything enabled
            new_schemas = [
                ConnectionSchema(
                    name=schema,
                    tables=[await self._build_connection_schema_table(session, schema, table, db, generate_columns
                                                                      , generate_descriptions) for table in tables],
                    enabled=True,
                )
                for schema, tables in db._all_tables_per_schema.items()
            ]
        else:
            # "schema_name": enabled
            existing_schemas: dict[str, bool] = {schema.name: schema.enabled for schema in old_options.schemas}
            # ("schema_name", "table_name"): enabled
            schema_table_enabled_map: dict[tuple[str, str], ConnectionSchemaTable] = {
                (schema.name, table.name): table for schema in old_options.schemas for table in schema.tables
            }

            new_schemas = [
                ConnectionSchema(
                    name=schema_name,
                    tables=[await self._build_connection_schema_table_from_existing(session, schema_name, table, db,
                                                                                    generate_columns,
                                                                                    generate_descriptions,
                                                                                    schema_table_enabled_map.get(
                                                                                        (schema_name, table), None))
                            for table in tables],
                    enabled=existing_schemas.get(schema_name, False),
                )
                for schema_name, tables in db._all_tables_per_schema.items()
            ]

        # sort schemas and tables by name
        new_schemas.sort(key=lambda x: x.name)
        for schema in new_schemas:
            schema.tables.sort(key=lambda x: x.name)

        return ConnectionOptions(schemas=new_schemas)

    async def refresh_connection_schema(self, session: AsyncSession, connection_id: UUID) -> ConnectionOut:
        """
        Refresh the schema of a connection. Flow of the function:
        1. Get the latest schema information from the database (using DatalineSQLDatabase)
        2.a. If ConnectionOptions is null in the db,  create new ConnectionOptions with everything enabled
        2.b. Otherwise, fetch stored ConnectionOptions from the database and merge with new schema information
        3. Sort schemas and tables by name
        4. Update the connection with new options
        """
        connection = await self.connection_repo.get_by_uuid(session, connection_id)

        # Get the latest schema information
        db = await self.get_db_from_dsn(connection.dsn)
        url = make_url(connection.dsn)
        query = url.query
        generate_columns = query.get("generate_columns", "false").lower() == "true"
        generate_descriptions = query.get("generate_descriptions", "false").lower() == "true"
        old_options = ConnectionOptions.model_validate(connection.options) if connection.options else None
        new_options = await self.merge_options(session, old_options, db, generate_columns, generate_descriptions)

        # Update the connection with new options
        updated_connection = await self.connection_repo.update_by_uuid(
            session, connection_id, ConnectionUpdate(options=new_options)
        )

        return ConnectionOut.model_validate(updated_connection)

    async def get_all_dicts(self, session:AsyncSession, connection_id: UUID) -> dict[str,list]:

        connection = await self.connection_repo.get_by_uuid(session, connection_id)

        the_dict = defaultdict(list)
        if connection.glossary:
            for gloss in connection.glossary:
                the_dict[gloss].append("glossary")

        if connection.unique_value_dict:
            for unique_key in connection.unique_value_dict:
                the_dict[unique_key].append("uniqueKey")

        return the_dict

    @classmethod
    async def generate_unique_value_dict(cls, options: ConnectionOptions):
        unique_values = defaultdict(list)
        for schema in options.schemas:
            for table in schema.tables:
                qualified_table_name = f"{schema.name}.{table.name}"

                for column in table.columns:
                    if column.reverse_look_up and column.possible_values:
                        for key in column.possible_values:
                            value_tuple = (column.name, qualified_table_name)
                            unique_values[key].append(value_tuple)
        return unique_values

