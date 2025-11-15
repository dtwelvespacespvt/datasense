import logging
import time
from typing import AsyncGenerator, Sequence, Type

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.runnables.config import RunnableConfig
from langchain_core.tracers.langchain import LangChainTracer
from langgraph.graph import StateGraph
from langgraph.prebuilt import ToolExecutor
from langsmith import Client

from dataline.config import config as dataline_config
from dataline.models.llm_flow.schema import QueryOptions, ResultType
from dataline.services.llm_flow.nodes import (
    CallModelNode,
    CallToolNode,
    Condition,
    Node,
    ShouldCallToolCondition, QueryValidationNode, ShouldCallModelCondition,
)
from dataline.services.llm_flow.prompt import SQL_PREFIX, LONG_TERM_MEMORY_MESSAGE
from dataline.services.llm_flow.toolkit import (
    ChartGeneratorTool,
    QueryGraphState,
    SQLDatabaseToolkit,
)
from dataline.services.llm_flow.utils import ConnectionProtocol, DatalineSQLDatabase as SQLDatabase
from dataline.utils.utils import forward_connection_errors

logger = logging.getLogger(__name__)


def add_node(graph: StateGraph, node: Type[Node]) -> None:
    graph.add_node(node.__name__, node.run)


def add_edge(graph: StateGraph, node_start: Type[Node], node_end: Type[Node]) -> None:
    graph.add_edge(node_start.__name__, node_end.__name__)


def add_conditional_edge(graph: StateGraph, source: Type[Node], condition: Type[Condition]) -> None:
    graph.add_conditional_edges(source.__name__, condition.run)


class QueryGraphService:
    def __init__(self, connection: ConnectionProtocol) -> None:
        # Enable this try catch once we support errors with streaming responses
        try:
            self.db = SQLDatabase.from_dataline_connection(connection)
        except Exception as e:
            forward_connection_errors(e)
            raise e
        self.connection = connection
        self.db._sample_rows_in_table_info = 0  # Preventative security
        self.toolkit = SQLDatabaseToolkit(db=self.db)
        all_tools = self.toolkit.get_tools() + [ChartGeneratorTool()]
        self.tool_executor = ToolExecutor(tools=all_tools)
        self.tracer = None  # no tracing by default

    async def query(
        self, query: str, options: QueryOptions, history: Sequence[BaseMessage] | None = None, long_term_memory: str | None =None
    ) -> AsyncGenerator[tuple[Sequence[BaseMessage] | None, Sequence[ResultType] | None], None]:
        # Setup tracing with langsmith if api key is provided
        if options.langsmith_api_key:
            self.tracer = LangChainTracer(client=Client(api_key=options.langsmith_api_key.get_secret_value()))

        if history is None:
            history = []

        graph = self.build_graph()
        app = graph.compile()
        image_bytes = app.get_graph().draw_mermaid_png(output_file_path="hello.png")
        if not options.secure_data:
            self.db._sample_rows_in_table_info = 3

        top_k = dataline_config.default_sql_row_limit

        if self.connection.config and self.connection.config.default_table_limit:
            top_k = self.connection.config.default_table_limit

        initial_state = {
            "messages": [
                *self.get_prompt_messages(query, history, top_k= top_k, long_term_memory = long_term_memory),
            ],
            "results": [],
            "options": options,
            "sql_toolkit": self.toolkit,
            "tool_executor": self.tool_executor,
            "validation_query": self.connection.config.validation_query if self.connection.config else None
        }

        config: RunnableConfig | None = {"callbacks": [self.tracer], "recursion_limit": 100} if self.tracer is not None else None
        current_results: Sequence[ResultType] | None
        current_messages: Sequence[BaseMessage] | None
        async for chunk in app.astream(initial_state, config=config):
            for tool, tool_chunk in chunk.items():
                current_results = tool_chunk.get("results")
                current_messages = tool_chunk.get("messages")
                yield (current_messages, current_results)

    def build_graph(self) -> StateGraph:
        # Create the graph
        graph = StateGraph(QueryGraphState)

        # Register nodes
        add_node(graph, CallModelNode)
        add_node(graph, CallToolNode)
        add_node(graph, QueryValidationNode)

        # Entry point
        graph.set_entry_point(QueryValidationNode.__name__)

        # Decision-making logic
        add_conditional_edge(graph, QueryValidationNode, ShouldCallModelCondition)
        add_conditional_edge(graph, CallModelNode, ShouldCallToolCondition)
        add_edge(graph, CallToolNode, CallModelNode)  # Loop back after tool use

        return graph

    def get_prompt_messages(
        self, query: str, history: Sequence[BaseMessage], top_k: int, suffix: str = LONG_TERM_MEMORY_MESSAGE, long_term_memory: str = None
    ):
        local_time = time.localtime()
        formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        prefix = SQL_PREFIX
        prefix = prefix.format(dialect=self.toolkit.dialect, top_k=top_k, connection_prompt=self.connection.config.connection_prompt if self.connection.config and self.connection.config.connection_prompt else "", current_time =str(formatted_time))
        messages = []
        if not history:
            messages = [
                SystemMessage(content=prefix),
                HumanMessage(content=query)
            ]
        else:
            messages = [
                SystemMessage(content=prefix),
                *history,
                HumanMessage(content=query)
            ]

        if long_term_memory:
            messages.append(SystemMessage(content=suffix.format(long_term_memory=long_term_memory)))

        return messages

