"""Antenna's bounded adapter to the existing Continuity JSON -> LangGraph compiler.

Each valve interrupts to request an existing Rust capability. Rust supplies the
answer on the next invocation. Checkpoints outlive this process. External
deliveries stay proposals until Antenna commits the completed graph and outbox.
"""
import json
import sys
from typing import TypedDict
from langgraph.types import Command, interrupt
from continuity.drawing import build


class ServiceState(TypedDict, total=False):
    input: dict
    result: dict
    outputs: dict
    deliveries: list


def resolve(value, state):
    if isinstance(value, dict):
        if set(value) == {"json_from"}:
            return json.dumps(resolve({"from":value["json_from"]},state),ensure_ascii=False,separators=(",",":"))
        if set(value) == {"from"}:
            result = state
            for key in value["from"].split("."):
                result = result[key]
            return result
        return {key: resolve(item, state) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve(item, state) for item in value]
    return value


def advance(request):
    drawing = request["graph"]
    nodes = drawing["nodes"]
    allowed = set(request["capabilities"])
    if not nodes or len(nodes) > 32 or not set(nodes.values()) <= allowed:
        raise ValueError("graph names unsupported capabilities or exceeds 32 nodes")
    if drawing["start"] not in nodes:
        raise ValueError("invalid graph start")
    # This service profile serializes effects. Multiple successors require an
    # explicit conditional branch; an accidental fan-out is never inferred.
    sources = [edge[0] for edge in drawing["edges"]]
    if len(sources) != len(set(sources)):
        raise ValueError("service graphs require one edge or one branch per node")
    if set(sources) & set(drawing.get("branches", {})):
        raise ValueError("a node cannot have both an edge and a conditional branch")
    successors = {source:[target] for source,target in drawing["edges"]}
    for source,branch in drawing.get("branches",{}).items():
        successors[source] = list(branch["routes"].values())
    if set(successors) != set(nodes):
        raise ValueError("each node needs an edge or branch, including an explicit END")
    visited, visiting = set(), set()
    def visit(node):
        if node == "END":
            return
        if node not in nodes or node in visiting:
            raise ValueError("unknown node or cycle in service graph")
        if node in visited:
            return
        visiting.add(node)
        for target in successors[node]:
            visit(target)
        visiting.remove(node)
        visited.add(node)
    visit(drawing["start"])
    if visited != set(nodes):
        raise ValueError("unreachable service graph node")
    valves = {}
    graph_spec = dict(drawing, nodes={name: name for name in nodes})
    for node, capability in nodes.items():
        def valve(state, node=node, capability=capability):
            params = resolve(drawing.get("parameters", {}).get(node, {"from":"input"}), state)
            response = interrupt({"node":node,"capability":capability,"params":params})
            if response["node"] != node:
                raise ValueError("resume belongs to another node")
            return {"result":response["result"],
                    "outputs":{**state.get("outputs",{}),node:response["result"]},
                    "deliveries":state.get("deliveries",[])+response.get("deliveries",[])}
        valves[node] = valve
    graph = build(graph_spec, valves, request["checkpoint"], ServiceState)
    config = {"configurable":{"thread_id":request["receipt_id"]},"recursion_limit":64}
    previous = graph.get_state(config)
    if "resume" in request:
        if not previous.next:
            raise ValueError("no suspended node to resume")
        graph.invoke(Command(resume=request["resume"]), config)
    elif not previous.values:
        graph.invoke({"input":request["input"],"outputs":{},"deliveries":[]}, config)
    current = graph.get_state(config)
    for task in current.tasks:
        for paused in task.interrupts:
            return {"status":"paused",**paused.value}
    if current.next:
        raise ValueError("checkpoint is unfinished without a capability request")
    return {"status":"completed","result":current.values.get("result"),
            "outputs":current.values.get("outputs",{}),"deliveries":current.values.get("deliveries",[])}


if __name__ == "__main__":
    try:
        print(json.dumps(advance(json.load(sys.stdin)), ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({"status":"failed","error":str(exc)}))
        sys.exit(1)
