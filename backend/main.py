from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Edge(BaseModel):
    from_node: str
    to_node: str

class Pipeline(BaseModel):
    nodes: List[Dict[str, str]] 
    edges: List[Edge] 

def is_dag(nodes, edges):
    adj_list = {node["id"]: [] for node in nodes}
    for edge in edges:
        adj_list[edge.from_node].append(edge.to_node)

    visited = set()
    stack = set()

    def dfs(node):
        if node in stack: 
            return False
        if node in visited:
            return True

        stack.add(node)
        for neighbor in adj_list[node]:
            if not dfs(neighbor):
                return False
        stack.remove(node)
        visited.add(node)
        return True

    for node in nodes:
        if node["id"] not in visited:
            if not dfs(node["id"]):
                return False
    return True

@app.post("/pipelines/parse")
def parse_pipeline(pipeline: Pipeline):
    num_nodes = len(pipeline.nodes)
    num_edges = len(pipeline.edges)
    is_dag_result = is_dag(pipeline.nodes, pipeline.edges)
    return {
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "is_dag": is_dag_result,
    }


@app.get("/")
def read_root():
    return {"Ping": "Pong"}
