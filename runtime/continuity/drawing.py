"""Turn a drawing (JSON: nodes, edges, branches) into a running LangGraph graph.

The drawing is the program. Python only supplies the named valves the drawing
refers to; a drawing that names an unknown valve is refused.
"""
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Callable, TypedDict

from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.graph import END, START, StateGraph


class State(TypedDict, total=False):
    place: str
    command: list[str]
    tier: str
    decision: str
    result: dict[str, Any]
    summary: str


Valve = Callable[[State], dict]


def branch_value(state, path):
    value = state
    for key in path.split("."):
        value = value[key]
    return value


def load(path: str | Path) -> tuple[dict, str]:
    raw = Path(path).read_bytes()
    return json.loads(raw), "sha256:" + hashlib.sha256(raw).hexdigest()


def build(drawing: dict, valves: dict[str, Valve], checkpoint_path: str | Path, state_type=State):
    unknown = sorted(set(drawing["nodes"].values()) - set(valves))
    if unknown:
        raise ValueError(f"drawing names valves that don't exist: {', '.join(unknown)}")

    graph = StateGraph(state_type)
    for node, valve in drawing["nodes"].items():
        graph.add_node(node, valves[valve])

    def target(name: str):
        return END if name == "END" else name

    graph.add_edge(START, drawing["start"])
    for source, dest in drawing["edges"]:
        graph.add_edge(source, target(dest))
    for source, branch in drawing.get("branches", {}).items():
        key, routes = branch["on"], branch["routes"]
        graph.add_conditional_edges(
            source,
            lambda state, key=key: branch_value(state, key),
            {value: target(dest) for value, dest in routes.items()},
        )

    conn = sqlite3.connect(str(checkpoint_path), check_same_thread=False)
    return graph.compile(checkpointer=SqliteSaver(conn))
