from preql.core.models import Environment, Concept, Function, DataType
from preql.core.enums import Purpose, FunctionType
from pypreqlt.enums import PreqltMetrics
from preql.core.functions import CurrentDatetime
from pypreqlt.constants import PREQLT_NAMESPACE

ENVIRONMENT_CONCEPTS = [
    Concept(
        name=PreqltMetrics.CREATED_AT.value,
        namespace=PREQLT_NAMESPACE,
        datatype=DataType.DATETIME,
        purpose=Purpose.CONSTANT,
        lineage=CurrentDatetime([]),
    )
]


def enrich_environment(env: Environment):
    if PREQLT_NAMESPACE in env.imports:
        raise Exception(f"{PREQLT_NAMESPACE} namespace already exists")

    for concept in ENVIRONMENT_CONCEPTS:
        env.add_concept(concept)
    return env
