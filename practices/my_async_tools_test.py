import asyncio
import json
import re
import uuid
from contextlib import AsyncExitStack

import requests
from langchain.tools import tool
from langchain.chat_models import init_chat_model
from langchain_core.messages import ToolMessage
from langchain_tavily import TavilySearch
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.graph.message import add_messages
from typing import Annotated, TypedDict
import load_tokens, os
from mcp import ClientSession
from mcp.client.sse import sse_client

def my_tools_condition(
    state,
    messages_key = "messages",
):
    if isinstance(state, list):
        ai_message = state[-1]
    elif isinstance(state, dict) and (messages := state.get(messages_key, [])):
        ai_message = messages[-1]
    elif messages := getattr(state, messages_key, []):
        ai_message = messages[-1]
    else:
        raise ValueError(f"No messages found in input state to tool_edge: {state}")

    if hasattr(ai_message, "tool_calls") and len(ai_message.tool_calls) > 0:
        return "tools"
    if hasattr(ai_message, "tool_calls"):
        pattern = r"```json\n(.*?)\n?```"
        match = re.search(pattern, ai_message.content, re.DOTALL)
        if match:
            llm_response = match.group(1)
            tool_call = json.loads(llm_response)
            if "tool" in tool_call and "arguments" in tool_call:
                # result = await self.session.call_tool(tool_name, tool_args)
                print(ai_message.content)
                print('going to call map')
                return "map_tools"
    return "__end__"

def stream_graph_updates(user_input: str, config):
    for event in graph.stream({"messages": [{"role": "user", "content": user_input}]}, config):
        for value in event.values():
            print("Assistant:", value["messages"][-1].content)

# one turn and extra
async def send_one_turn_message(graph, config, message, with_system=False):
    try:
        if not with_system:
            events = graph.stream(
                {"messages": [{"role": "user", "content": message}]},
                config,
                stream_mode="values",
            )
        else:
            events = graph.stream(
                {"messages": [{"role": "system", "content": with_system}, {"role": "user", "content": message}]},
                config,
                stream_mode="values",
            )
        for event in events:
            event["messages"][-1].pretty_print()
            print()
        print()
    except Exception as e:
        print(e)

class AmapClient:
    session: ClientSession = None
    _lock = asyncio.Lock()
    is_connected = False
    # close

amap_client = AmapClient()

@tool
def embellish(text: str):
    """对文本进行润色和扩展。"""
    prompt = f'请对以下内容进行添油加醋，要求夸张、搞笑、无厘头、浮夸，但是输出一定要是中文：“{text}”'
    result = llm_with_tools.invoke(prompt)
    return result

search_tool = TavilySearch(max_results=2)

async def connect_amap_server():
    """
    在进行 amap 工具调用之前，需要先连接 amap 服务器，看看有什么 amap 工具，然后才能按照指示调用 amap 工具
    """
    def format_tools_for_llm(tool) -> str:
        """对tool进行格式化
        Returns:
            格式化之后的tool描述
        """
        args_desc = []
        if "properties" in tool.inputSchema:
            for param_name, param_info in tool.inputSchema["properties"].items():
                arg_desc = (
                    f"- {param_name}: {param_info.get('description', 'No description')}"
                )
                if param_name in tool.inputSchema.get("required", []):
                    arg_desc += " (required)"
                args_desc.append(arg_desc)

        return f"Tool: {tool.name}\nDescription: {tool.description}\nArguments:\n{chr(10).join(args_desc)}"
    async with amap_client._lock:  # 防止并发调用 connect
        url = "https://mcp.amap.com/sse?key=4afc5e4560f5866faf466f9c6531a447"
        print(f"尝试连接到: {url}")
        amap_client._exit_stack = AsyncExitStack()
        # 1. 进入 SSE 上下文，但不退出
        sse_cm = sse_client(url)
        # 手动调用 __aenter__ 获取流，并存储上下文管理器以便后续退出
        streams = await amap_client._exit_stack.enter_async_context(sse_cm)
        print("SSE 流已获取。")

        # 2. 进入 Session 上下文，但不退出
        session_cm = ClientSession(streams[0], streams[1])
        # 手动调用 __aenter__ 获取 session
        amap_client.session = await amap_client._exit_stack.enter_async_context(session_cm)
        print("ClientSession 已创建。")

        # 3. 初始化 Session
        await amap_client.session.initialize()
        print("Session 已初始化。")

        # 4. 获取并存储工具列表
        response = await amap_client.session.list_tools()
        amap_client.tools = {tool.name: tool for tool in response.tools}
        print(f"成功获取 {len(amap_client.tools)} 个工具:")
        for name, tool in amap_client.tools.items():
            print(f"  - {name}: {tool.description[:50]}...")  # 打印部分描述

        print("连接成功并准备就绪。")

    # 列出可用工具
    response = await amap_client.session.list_tools()
    tools = response.tools

    tools_description = "\n".join([format_tools_for_llm(tool) for tool in tools])
    # 修改系统提示
    amap_system_prompt = (
        "You are a helpful assistant with access to these tools:\n\n"
        f"{tools_description}\n"
        "Choose the appropriate tool based on the user's question. "
        "If no tool is needed, reply directly.\n\n"
        "IMPORTANT: When you need to use a tool to solve map related request, you must ONLY respond with "
        "the exact JSON object format below, nothing else:\n"
        "{\n"
        '    "tool": "tool-name",\n'
        '    "arguments": {\n'
        '        "argument-name": "value"\n'
        "    }\n"
        "}\n\n"
        '"```json" is not allowed'
        "After receiving a tool's response:\n"
        "1. Transform the raw data into a natural, conversational response\n"
        "2. Keep responses concise but informative\n"
        "3. Focus on the most relevant information\n"
        "4. Use appropriate context from the user's question\n"
        "5. Avoid simply repeating the raw data\n\n"
        "But if you are not dueling with a query about map, you don't have to follow the order above"
    )
    return amap_system_prompt

@tool#(infer_schema=False)
async def amap_search_tool(llm_response: str):
    """
    用高德mcp进行行程相关的工具调用
    """
    import json

    # try:
    #     assert isinstance(amap_client, AmapClient)
    # except AssertionError:
    #     print("还没有调用 connect_amap_server 工具进行客户端连接，请先进行 amap 客户端连接")
    #     return "还没有调用 connect_amap_server 工具进行客户端连接，请先进行 amap 客户端连接"

    try:
        pattern = r"```json\n(.*?)\n?```"
        match = re.search(pattern, llm_response, re.DOTALL)
        if match:
            llm_response = match.group(1)
        tool_call = json.loads(llm_response)
        if "tool" in tool_call and "arguments" in tool_call:
            # result = await self.session.call_tool(tool_name, tool_args)
            response = await amap_client.session.list_tools()
            tools = response.tools

            if any(tool.name == tool_call["tool"] for tool in tools):
                try:
                    print(f"[提示]：正在调用工具 {tool_call['tool']}")
                    result = await amap_client.session.call_tool(
                        tool_call["tool"], tool_call["arguments"]
                    )

                    if isinstance(result, dict) and "progress" in result:
                        progress = result["progress"]
                        total = result["total"]
                        percentage = (progress / total) * 100
                        print(f"Progress: {progress}/{total} ({percentage:.1f}%)")
                    # print(f"[执行结果]: {result}")
                    return ToolMessage(content=f"Tool execution result: {result}", tool_call_id=str(uuid.uuid4()))
                except Exception as e:
                    error_msg = f"Error executing tool: {str(e)}"
                    print(error_msg)
                    return ToolMessage(content=error_msg, tool_call_id=str(uuid.uuid4()))

            return ToolMessage(content=f"No server found with tool: {tool_call['tool']}", tool_call_id=str(uuid.uuid4()))
        return ToolMessage(content=llm_response, tool_call_id=str(uuid.uuid4()))
    except json.JSONDecodeError:
        return ToolMessage(content=llm_response, tool_call_id=str(uuid.uuid4()))

class State(TypedDict):
    messages: Annotated[list, add_messages]

def chatbot(state: State):
    result = {"messages": [llm_with_tools.invoke(state["messages"])]}
    return result


GEMINI_MODEL = "gemini-2.0-flash"
llm = init_chat_model(GEMINI_MODEL, model_provider='google_genai')
# llm_with_tools = llm.bind_tools([search_tool, embellish, connect_amap_server, amap_search_tool])
llm_with_tools = llm.bind_tools([search_tool, embellish, amap_search_tool])


# 初始化图
graph_builder = StateGraph(State)

# 添加节点
graph_builder.add_node("chatbot", chatbot)
graph_builder.add_node("tools", ToolNode([search_tool, embellish, amap_search_tool]))
# graph_builder.add_node("map_tools", ToolNode([amap_search_tool]))

# 添加条件边，根据模型输出决定是否调用工具
graph_builder.add_conditional_edges(
    "chatbot",
    my_tools_condition,
    {
        # "text_tools": "text_tools",
        # "map_tools": "map_tools",
        "tools": "tools",
        # "map_tools": "map_tools",
        END: END
    }
)

# 添加工具节点到 chatbot 的边
graph_builder.add_edge("tools", "chatbot")


# 设置入口点
graph_builder.set_entry_point("chatbot")

# 编译图
from langgraph.checkpoint.memory import MemorySaver

memory = MemorySaver()
graph = graph_builder.compile(checkpointer=memory)


async def main():
    system_prompt = await connect_amap_server()
    config = {"configurable": {"thread_id": "1"}}
    snapshot = graph.get_state(config)
    # snapshot["messages"].append({
    #     "role": "system",
    #     "content": system_prompt
    # })

    # await send_one_turn_message(graph, config, '请搜索骆宾王的咏鹅的原文', with_system=system_prompt)
    # await send_one_turn_message(graph, config, '请搜索骆宾王的咏鹅的原文')
    # await send_one_turn_message(graph, config,'请对原文使用添油加醋工具进行添油加醋')

    # send_one_turn_message(graph, config, '你是一个多功能工具，可以帮我完成不同的请求。如果我请你帮我完成简单的搜索，请用 text_tools，如果是复杂的地图相关的搜索，比如规划行程，请使用 map_tools，如果不知道用什么工具，或者你认为没有合适的工具，请自己根据模型本身的知识回答')
    await send_one_turn_message(graph, config, '我要去广州花城汇出差，请你用 amap 查询附近5km的酒店，为我安排行程', with_system=system_prompt)
    print()

    while True:
        try:
            user_input = input("User: ")
            if user_input.lower() in ["quit", "exit", "q", "end the chat", "结束对话"]:
                print("Goodbye!")
                break

            stream_graph_updates(user_input, config)
        except Exception as e:
            # fallback if input() is not available
            raise KeyError(e)
            user_input = "What do you know about LangGraph?"
            print("User: " + user_input)
            stream_graph_updates(user_input, config)
            break


if __name__ == "__main__":
    asyncio.run(main())

