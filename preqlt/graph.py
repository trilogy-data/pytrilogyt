from preql.core.models import (
    Select,
    Persist,
    QueryDatasource,
    Datasource,
    ProcessedQuery,
    Grain,
    ProcessedQueryPersist,
    CompiledCTE,
    CTE,
    Concept,
)
from typing import List
from collections import defaultdict
from preql import Executor, Environment
from preql.dialect.bigquery import BigqueryDialect

def fingerprint_concept(concept: Concept) -> str:
    return concept.address


def fingerprint_grain(grain: Grain) -> str:
    return "-".join([fingerprint_concept(x) for x in grain.components])


def fingerprint_source(source: QueryDatasource | Datasource) -> str:
    if isinstance(source, Datasource):
        return str(source.address)
    return "-".join([fingerprint_source(s) for s in source.datasources]) + str(
        source.limit
    )


def fingerprint_cte(cte: CTE) -> str:
    return (
        fingerprint_source(cte.source)
        + "-".join([fingerprint_cte(x) for x in cte.parent_ctes])
        + fingerprint_grain(cte.grain)
    )


def process_raw(inputs: List[Select | Persist], env:Environment):
    parsed = BigqueryDialect().generate_queries(env, inputs)
    return process(parsed, env)

def process(inputs: List[ProcessedQuery | ProcessedQueryPersist], env:Environment):
    counts = {}
    valid = [c for c in inputs if isinstance(c, (ProcessedQuery, ProcessedQueryPersist))]
    if not valid:
        raise ValueError("No valid queries found")
    for x in valid:
        for cte in x.ctes:
            fingerprint = fingerprint_cte(cte)
            counts[fingerprint] = counts.get(fingerprint, 0) + 1
    for k, v in counts.items():
        if v > 1:
            print(f"Warning: CTE {k} is used {v} times")
    return counts