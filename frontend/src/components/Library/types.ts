// Enum for sql dialects
export enum Dialect {
  Postgres = "postgresql",
  MySQL = "mysql",
  MySQLServer = "mssql",
  SQLite = "sqlite",
  MariaDB = "mariadb",
  Spark = "spark",
  BigQuery = "bigquery",
  IBMDB2 = "db2",
  Hive = "hive",
  Couchbase = "n1ql",
  TransactSQL = "tsql",
}

export enum UserRole {
  Admin = "ADMIN",
  User = "USER",
}

export type IResultTypeName =
  | "SQL_QUERY_STRING_RESULT"
  | "SQL_QUERY_RUN_RESULT"
  | "SELECTED_TABLES"
  | "CHART_GENERATION_RESULT"
  | "TOOL_REASON";

export type DatabaseFileType = "sqlite" | "csv" | "sas7bdat" | "excel";

export type Role = "ai" | "human";

export interface IResult {
  type: IResultTypeName;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  content: any;
  created_at?: string;
}
export interface ISelectedTablesResult extends IResult {
  type: "SELECTED_TABLES";
  result_id: string;
  linked_id: string;
  content: {
    tables: string[];
  };
}

export interface IToolReasonResult extends IResult {
  type: "TOOL_REASON";
  result_id: string;
  linked_id: string;
  content: {
    reason: string;
  };
}

export interface ISQLQueryStringResult extends IResult {
  type: "SQL_QUERY_STRING_RESULT";
  result_id: string;
  content: {
    dialect: string;
    sql: string;
    for_chart: boolean;
  };
}

export interface IChartGenerationResult extends IResult {
  type: "CHART_GENERATION_RESULT";
  result_id: string;
  linked_id: string;
  content: {
    chartjs_json: string;
  };
}

export interface ISQLQueryRunResult extends IResult {
  type: "SQL_QUERY_RUN_RESULT";
  result_id: string;
  linked_id: string;
  content: {
    columns: string[];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    rows: any[][];
  };
}

export interface IMessageOptions {
  secure_data: boolean;
  debug: boolean;
}

export interface IMessageOut {
  id?: string;
  content: string;
  role: Role;
  created_at?: string;
  is_positive?: boolean;
  options?: IMessageOptions;
}

export enum QueryStreamingEvent {
  STORED_MESSAGES = "stored_messages_event",
  ADD_RESULT = "add_result_event",
  ERROR = "error_event",
}

export type IResultType =
  | ISQLQueryRunResult
  | ISQLQueryStringResult
  | ISelectedTablesResult
  | IChartGenerationResult
  | IToolReasonResult;
export interface IMessageWithResultsOut {
  message: IMessageOut;
  results?: IResultType[];
}
export interface IConversationWithMessagesWithResultsOut {
  id: string;
  connection_id: string;
  name: string;
  messages: IMessageWithResultsOut[];
}

export interface IConversation {
  id: string;
  name: string;
}

export interface IConnectionOptions {
  schemas: {
    name: string;
    enabled: boolean;
    tables: {
      name: string;
      enabled: boolean;
      columns?: {
        name?: string;
        type?: string;
        possible_values?: string[];
        primary_key?: boolean;
        description?: string;
        enabled?: boolean;
        reverse_look_up?: boolean;
        relationship?: {
          schema_name?: string;
          table?: string;
          column?: string;
          enabled?: boolean;
        }[];
      }[];
      description?: string;
    }[];
  }[];
}

export interface ConnectionConfig {
  connection_prompt?: string;
  validation_query?: string;
  default_table_limit?: number;
}

export interface IConnection {
  id: string;
  dsn: string;
  database: string;
  name: string;
  dialect: string;
  is_sample: boolean;
  options?: IConnectionOptions;
  glossary?: object;
  config?: ConnectionConfig;
}

export interface IEditConnection {
  name: string;
  dsn?: string;
  options?: IConnectionOptions;
  glossary?: object;
  config?: ConnectionConfig
}

export interface UserConfig {
  connections: string[];
}
export interface IUserInfo {
  id?: string;
  name: string;
  email?: string;
  openai_api_key: string;
  openai_base_url?: string | null;
  langsmith_api_key?: string | null;
  preferred_openai_model?: string;
  sentry_enabled: boolean;
  analytics_enabled: boolean;
  role: string;
  config?: {
    connections?: string[];
  } | null;
}

export interface MessageFeedbackUpdate {
  message_id: string;
  is_positive: boolean;
  content: string;
}

export interface PossibleValuesResult {
  possibleValues: []
}
export interface RelationshipsResult {
  relationships: []
}

export interface GlossaryItem {
  key: string;
  value: string;
}
export interface DictionaryResult {
  glossary: GlossaryItem
}


