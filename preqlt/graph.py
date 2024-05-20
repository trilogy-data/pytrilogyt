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
    Conditional,
    WhereClause,
)
from typing import List
from preql import Environment
from preql.core.ergonomics import CTE_NAMES
from random import choice
from dataclasses import dataclass
from preql.dialect.base import BaseDialect


@dataclass
class AnalyzeResult:
    counts: dict[str, str]
    mapping: dict[str, CTE]


@dataclass
class ProcessLoopResult:
    new: List[ProcessedQueryPersist]

def generate_datasource_name(select:Select, cte_name:str)->str:
    """Generate a reasonable table name"""
    base = '_'.join([x.name for x in select.grain.components])
    if select.where_clause:
        base = base + "_filtered_on_" + '_'.join([x.name for x in select.where_clause.conditional.concept_arguments])
    return cte_name +'_'+base

def cte_to_persist(input: CTE, name: str, dialect: BaseDialect) -> Persist:
    select = Select(
        selection=input.output_columns,
        where_clause=(
            WhereClause(condition=input.condition) if input.condition else None
        ),
    )
    datasource = select.to_datasource(
        namespace="default",
        identifier=generate_datasource_name(select, name),
        address=Address(location=name),
    )
    persist = Persist(datasource=datasource, select=select)
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

def fingerprint_filter(filter: Conditional) -> str:
    return str(filter)

def fingerprint_cte(cte: CTE) -> str:
    if cte.condition:
        return (fingerprint_source(cte.source) 
        + "-".join([fingerprint_cte(x) for x in cte.parent_ctes])
        + fingerprint_grain(cte.grain)
        + fingerprint_filter(cte.condition))
    return (
        fingerprint_source(cte.source)
        + "-".join([fingerprint_cte(x) for x in cte.parent_ctes])
        + fingerprint_grain(cte.grain)
    )


def process_raw(
    inputs: List[Select | Persist],
    env: Environment,
    dialect: BaseDialect,
    threshold: int = 2,
):
    complete = False
    while not complete:
        parsed = dialect.generate_queries(env, inputs)
        new = process_loop(parsed, env, dialect=dialect,threshold=threshold)
        if new.new:
            insert_index = min([inputs.index(x) for x in inputs if isinstance(x, Select)])
            # insert the new values before this statement
            inputs = inputs[:insert_index] + new.new + inputs[insert_index:]
            # hardcoded until we find out the exit criteria for multipass
            complete = True
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
    final_map: dict[str, CTE] = {}
    for fingerprint, cte in lookup.items():
        final_map[mapping[fingerprint]] = cte
    return AnalyzeResult(counts, final_map)

def is_raw_source_cte(cte:CTE)->bool:
    if not len(cte.source.datasources) == 1:
        return False
    if isinstance(cte.source.datasources[0], Datasource):
        return True

def process_loop(
    inputs: List[ProcessedQuery | ProcessedQueryPersist | ProcessedShowStatement],
    env: Environment,
    dialect:BaseDialect,
    threshold: int = 2,
) -> ProcessLoopResult:
    analysis = analyze(inputs, env)
    new_persist = []
    for k, v in analysis.counts.items():
        if v > threshold:
            cte = analysis.mapping[k]
            # skip materializing materialized things
            if is_raw_source_cte(cte):
                continue
            new_persist.append(cte_to_persist(cte, k, dialect=dialect))

    return ProcessLoopResult(new=new_persist)
