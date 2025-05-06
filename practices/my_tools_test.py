from langchain.tools import tool
from langchain.chat_models import init_chat_model
from langchain_tavily import TavilySearch
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.graph.message import add_messages
from typing import Annotated, TypedDict
import load_tokens, os




def stream_graph_updates(user_input: str, config):
    for event in graph.stream({"messages": [{"role": "user", "content": user_input}]}, config):
        for value in event.values():
            print("Assistant:", value["messages"][-1].content)

@tool
def embellish(text: str):
    """对文本进行润色和扩展。"""
    prompt = f'请对以下内容进行添油加醋，要求夸张、搞笑、无厘头、浮夸，但是输出一定要是中文：“{text}”'
    result = llm_with_tools.invoke(prompt)
    return result

class State(TypedDict):
    messages: Annotated[list, add_messages]

def chatbot(state: State):
    return {"messages": [llm_with_tools.invoke(state["messages"])]}

search_tool = TavilySearch(max_results=2)

GEMINI_MODEL = "gemini-2.0-flash"
llm = init_chat_model(GEMINI_MODEL, model_provider='google_genai')
llm_with_tools = llm.bind_tools([search_tool, embellish])

# 初始化图
graph_builder = StateGraph(State)

# 添加节点
graph_builder.add_node("chatbot", chatbot)
graph_builder.add_node("tools", ToolNode([search_tool, embellish]))

# 添加条件边，根据模型输出决定是否调用工具
graph_builder.add_conditional_edges(
    "chatbot",
    tools_condition,
    {"tools": "tools", END: END}
)

# 添加工具节点到 chatbot 的边
graph_builder.add_edge("tools", "chatbot")

# 设置入口点
graph_builder.set_entry_point("chatbot")

# 编译图
from langgraph.checkpoint.memory import MemorySaver

memory = MemorySaver()
graph = graph_builder.compile(checkpointer=memory)




if __name__ == "__main__":
    config = {"configurable": {"thread_id": "1"}}
    snapshot = graph.get_state(config)

    # one turn chat
    # try:
    #     events = graph.stream(
    #         {"messages": [{"role": "user", "content": '请搜索骆宾王的咏鹅'}]},
    #         config,
    #         stream_mode="values",
    #     )
    #     for event in events:
    #         event["messages"][-1].pretty_print()
    #         print()
    #     print()
    # except Exception as e:
    #     print(e)

    # loop chat

    #
    #
    # while True:
    #     try:
    #         user_input = input("User: ")
    #         if user_input.lower() in ["quit", "exit", "q", "end the chat", "结束对话"]:
    #             print("Goodbye!")
    #             break
    #
    #         stream_graph_updates(user_input, config)
    #     except Exception as e:
    #         # fallback if input() is not available
    #         user_input = "What do you know about LangGraph?"
    #         print("User: " + user_input)
    #         stream_graph_updates(user_input, config)
    #         break

    # one turn and extra
    try:
        events = graph.stream(
            {"messages": [{"role": "user", "content": '请搜索骆宾王的咏鹅的原文'}]},
            config,
            stream_mode="values",
        )
        for event in events:
            event["messages"][-1].pretty_print()
            print()
        print()
    except Exception as e:
        print(e)

    try:
        events = graph.stream(
            {"messages": [{"role": "user", "content": '请对原文使用添油加醋工具进行添油加醋'}]},
            config,
            stream_mode="values",
        )
        for event in events:
            event["messages"][-1].pretty_print()
            print()
        print()
    except Exception as e:
        print(e)

    while True:
        try:
            user_input = input("User: ")
            if user_input.lower() in ["quit", "exit", "q", "end the chat", "结束对话"]:
                print("Goodbye!")
                break

            stream_graph_updates(user_input, config)
        except Exception as e:
            # fallback if input() is not available
            user_input = "What do you know about LangGraph?"
            print("User: " + user_input)
            stream_graph_updates(user_input, config)
            break
