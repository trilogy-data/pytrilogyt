from hashlib import sha256

from trilogy.authoring import Concept, DataType, Environment
from trilogy.core.enums import Purpose
from trilogy.core.env_processor import generate_graph
from trilogy.core.functions import CurrentDatetime
from trilogy.core.models.build_environment import BuildEnvironment

from trilogyt.constants import TRILOGY_NAMESPACE
from trilogyt.enums import PreqltMetrics

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


def fingerprint_environment(env: BuildEnvironment) -> str:
    graph = generate_graph(env)
    edges = "-".join([str(x) for x in sorted(list(graph.edges))])
    return sha256(edges.encode()).hexdigest()
