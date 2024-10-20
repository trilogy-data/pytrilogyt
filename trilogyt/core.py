from trilogy.core.models import Environment, Concept, DataType
from trilogy.core.enums import Purpose
from trilogyt.enums import PreqltMetrics
from trilogy.core.functions import CurrentDatetime
from trilogyt.constants import TRILOGY_NAMESPACE
from trilogy.core.env_processor import generate_graph
from hashlib import sha256

ENVIRONMENT_CONCEPTS = [
    Concept(
        name=PreqltMetrics.CREATED_AT.value,
        namespace=TRILOGY_NAMESPACE,
        datatype=DataType.DATETIME,
        purpose=Purpose.CONSTANT,
        lineage=CurrentDatetime([]),
    )
]


def enrich_environment(env: Environment):
    if TRILOGY_NAMESPACE in env.imports:
        raise Exception(f"{TRILOGY_NAMESPACE} namespace already exists")

    for concept in ENVIRONMENT_CONCEPTS:
        env.add_concept(concept)
    return env


def fingerprint_environment(env:Environment)->str:
    graph = generate_graph(env)
    edges = '-'.join([str(x) for x in sorted(list(graph.edges))])
    return sha256(edges.encode()).hexdigest()