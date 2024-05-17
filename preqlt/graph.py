from preql.core.models import (
    Address,
    Select,
    Persist,
    QueryDatasource,
    Datasource,
    ProcessedQuery,
    Grain,
    ProcessedQueryPersist,
    ProcessedShowStatement,
    CTE,
    Concept,
    WhereClause
)
from typing import List
from preql import Environment
from preql.core.ergonomics import CTE_NAMES
from random import choice
from dataclasses import dataclass
from preql.dialect.base import BaseDialect

@dataclass
class AnalyzeResult:
    counts:dict[str,str]
    mapping:dict[str, CTE]

@dataclass
class ProcessLoopResult:
    new: List[ProcessedQueryPersist]

def cte_to_persist(input:CTE, name:str, dialect:BaseDialect)->Persist:
    select=Select(
            selection = input.output_columns,
            where_clause = WhereClause(condition = input.condition) if input.condition else None,
        )
    datasource = select.to_datasource(namespace = 'default',
                                      identifier=name, address= Address(location=name),
                                    )
    persist = Persist(
        datasource=datasource,
        select=select
    )
    return persist

def remap_dictionary(d: dict, remap: list[str]) -> tuple[dict, dict]:
    new: dict[str, str] = {}
    mapping: dict[str, CTE] = {}
    for k, v in d.items():
        alias = None
        while alias is None or alias in new:
            alias = choice(remap)
        new[alias] = v
        mapping[k] = alias
    return new, mapping


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


def process_raw(inputs: List[Select | Persist], env: Environment, dialect:BaseDialect,
                threshold:int=2):
    complete = False
    while not complete:
        parsed = dialect.generate_queries(env, inputs)
        new = process_loop(parsed, env, dialect, threshold)
        if new.new:
            inputs = new.new + parsed
        else:
            complete = True
    return inputs


def analyze(
    inputs: List[ProcessedQuery | ProcessedQueryPersist | ProcessedShowStatement],
    env: Environment,
):
    counts: dict[str, int] = {}
    lookup: dict[str, CTE] = {}
    valid = [
        c for c in inputs if isinstance(c, (ProcessedQuery, ProcessedQueryPersist))
    ]
    if not valid:
        raise ValueError("No valid queries found")
    for x in valid:
        for cte in x.ctes:
            fingerprint = fingerprint_cte(cte)
            counts[fingerprint] = counts.get(fingerprint, 0) + 1
            lookup[fingerprint] = lookup.get(fingerprint, cte) + cte
    for k, v in counts.items():
        print(f"CTE {k} is used {v} times")
    counts, mapping = remap_dictionary(counts, CTE_NAMES)
    for k, v in counts.items():
        print(f"CTE {k} is used {v} times")
    return AnalyzeResult(counts, mapping)

def process_loop(    inputs: List[ProcessedQuery | ProcessedQueryPersist | ProcessedShowStatement],
    env: Environment,
    dialect:BaseDialect,
    threshold:int =2)->ProcessLoopResult:
    analysis = analyze( inputs, env)
    new_persist = []
    for k, v in analysis.counts.items():
        if v>threshold:
            new_persist.append(cte_to_persist(analysis.mapping[k]))


    return ProcessLoopResult(
        new=new_persist
    )