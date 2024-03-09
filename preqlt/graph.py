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


def fingerprint_concept(concept: Concept) -> str:
    return concept.address


def fingerprint_grain(grain: Grain) -> str:
    return "-".join([fingerprint_concept(x) for x in grain.components])


def fingerprint_source(source: QueryDatasource | Datasource) -> str:
    if isinstance(source, Datasource):
        return source.address
    return "-".join([fingerprint_source(s) for s in source.datasources]) + str(
        source.limit
    )


def fingerprint_cte(cte: CTE) -> str:
    return (
        fingerprint_source(cte.source)
        + "-".join([fingerprint_cte(x) for x in cte.parent_ctes])
        + fingerprint_grain(cte.grain)
    )


def process_raw(inputs: List[Select | Persist]):
    pass


def process(inputs: List[ProcessedQuery | ProcessedQueryPersist]):
    counts = {}
    for x in inputs:
        for cte in x.ctes:
            fingerprint = fingerprint_cte(cte)
            counts[fingerprint] = counts.get(fingerprint, 0) + 1
    for k, v in counts:
        if v > 1:
            print(f"Warning: CTE {k} is used {v} times")
    return counts