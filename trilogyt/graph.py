from trilogy.core.models import (
    Address,
    SelectStatement,
    PersistStatement,
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
    Comparison,
    Parenthetical,
)
from typing import List
from trilogy import Environment
from trilogy.core.ergonomics import CTE_NAMES
from trilogy.constants import VIRTUAL_CONCEPT_PREFIX
from random import choice
from dataclasses import dataclass
from trilogy.dialect.base import BaseDialect
from hashlib import sha256


@dataclass
class AnalyzeResult:
    counts: dict[str, int]
    mapping: dict[str, CTE]


@dataclass
class ProcessLoopResult:
    new: List[PersistStatement]


def name_to_short_name(x: str):
    if x.startswith(VIRTUAL_CONCEPT_PREFIX):
        return "virtual"
    return x


# hash a
def hash_concepts(concepts: list[Concept]) -> str:
    return sha256("".join([x.name for x in concepts]).encode()).hexdigest()


def generate_datasource_name(select: SelectStatement, cte_name: str) -> str:
    """Generate a reasonable table name"""
    base = "_".join(
        [name_to_short_name(x.name) for x in select.grain.components]
        + [hash_concepts(select.output_components)]
    )
    if select.where_clause:
        base = (
            base
            + "_filtered_on_"
            + "_".join(
                [
                    name_to_short_name(x.name)
                    for x in select.where_clause.conditional.concept_arguments
                ]
            )
        )
    return base


def cte_to_persist(input: CTE, name: str, generator: BaseDialect) -> PersistStatement:
    select = SelectStatement(
        selection=input.output_columns,
        where_clause=(
            WhereClause(conditional=input.condition) if input.condition else None
        ),
    )
    datasource = select.to_datasource(
        namespace="default",
        identifier=generate_datasource_name(select, name),
        address=Address(location=generate_datasource_name(select, name)),
    )
    persist = PersistStatement(datasource=datasource, select=select)
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


def fingerprint_filter(filter: Conditional | Comparison | Parenthetical) -> str:
    return str(filter)


def fingerprint_cte(cte: CTE) -> str:
    if cte.condition:
        return (
            fingerprint_source(cte.source)
            + "-".join([fingerprint_cte(x) for x in cte.parent_ctes])
            + fingerprint_grain(cte.grain)
            + fingerprint_filter(cte.condition)
        )
    return (
        fingerprint_source(cte.source)
        + "-".join([fingerprint_cte(x) for x in cte.parent_ctes])
        + fingerprint_grain(cte.grain)
    )


def process_raw(
    inputs: List[SelectStatement | PersistStatement],
    env: Environment,
    generator: BaseDialect,
    threshold: int = 2,
    inject: bool = False,
) -> tuple[list[SelectStatement | PersistStatement], list[PersistStatement]]:
    complete = False
    outputs = []
    while not complete:
        parsed = generator.generate_queries(env, inputs)
        new = process_loop(parsed, env, generator=generator, threshold=threshold)
        if new.new:
            outputs += new.new
            # hardcoded until we figure out the right exit criteria
            complete = True
            if inject:
                insert_index = min(
                    [inputs.index(x) for x in inputs if isinstance(x, SelectStatement)]
                )
                # insert the new values before this statement
                inputs = inputs[:insert_index] + new.new + inputs[insert_index:]
        else:
            complete = True
    return inputs, outputs


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


def is_raw_source_cte(cte: CTE) -> bool:
    if not len(cte.source.datasources) == 1:
        return False
    if isinstance(cte.source.datasources[0], Datasource):
        return True
    return False


def has_anon_concepts(cte: CTE) -> bool:
    if any([x.name.startswith(VIRTUAL_CONCEPT_PREFIX) for x in cte.output_columns]):
        return True
    return False


def process_loop(
    inputs: List[ProcessedQuery | ProcessedQueryPersist | ProcessedShowStatement],
    env: Environment,
    generator: BaseDialect,
    threshold: int = 2,
) -> ProcessLoopResult:
    analysis = analyze(inputs, env)
    new_persist = []
    for k, v in analysis.counts.items():
        if v >= threshold:
            cte = analysis.mapping[k]
            # skip materializing materialized things
            if is_raw_source_cte(cte):
                continue
            if has_anon_concepts(cte):
                continue
            new_persist.append(cte_to_persist(cte, k, generator=generator))

    return ProcessLoopResult(new=new_persist)
