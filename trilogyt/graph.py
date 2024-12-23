from dataclasses import dataclass
from hashlib import sha256
from random import choice
from typing import List

from trilogy import Environment
from trilogy.constants import VIRTUAL_CONCEPT_PREFIX
from trilogy.core.enums import PurposeLineage
from trilogy.core.ergonomics import CTE_NAMES
from trilogy.core.models import (
    CTE,
    Address,
    Comparison,
    Concept,
    Conditional,
    Datasource,
    Grain,
    Parenthetical,
    PersistStatement,
    ProcessedQuery,
    ProcessedQueryPersist,
    ProcessedRawSQLStatement,
    ProcessedShowStatement,
    QueryDatasource,
    RowsetDerivationStatement,
    SelectItem,
    SelectStatement,
    UnionCTE,
    WhereClause,
)
from trilogy.dialect.base import BaseDialect


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
    return sha256("".join([x.address for x in concepts]).encode()).hexdigest()


def generate_datasource_name(select: SelectStatement, cte_name: str) -> str:
    """Generate a reasonable table name"""
    base = "ds" + "_".join(
        [name_to_short_name(x) for x in select.grain.components]
        + [hash_concepts(select.output_components)]
    )
    if select.where_clause:
        # TODO: needs to include the filter predicates
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

    # a rowset will have the CTE ported over and we can assume the select statement doesn't need
    # further filtering
    rowset = any([x.derivation == PurposeLineage.ROWSET for x in input.output_columns])
    if rowset:
        select = SelectStatement(
            selection=[SelectItem(content=x) for x in input.output_columns],
        )
    else:
        select = SelectStatement(
            selection=[SelectItem(content=x) for x in input.output_columns],
            where_clause=(
                WhereClause(conditional=input.condition) if input.condition else None
            ),
        )
    datasource = select.to_datasource(
        namespace="default",
        name=generate_datasource_name(select, name),
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
    return "-".join([x for x in grain.components])


def fingerprint_source(source: QueryDatasource | Datasource) -> str:
    if isinstance(source, Datasource):
        return str(source.address)
    return "-".join([fingerprint_source(s) for s in source.datasources]) + str(
        source.limit
    )


def fingerprint_filter(filter: Conditional | Comparison | Parenthetical) -> str:
    return str(filter)


def fingerprint_cte(cte: CTE | UnionCTE, select_condition) -> str:
    if isinstance(cte, UnionCTE):
        return "-".join(
            [fingerprint_cte(x, select_condition) for x in cte.internal_ctes]
        )
    else:
        base = (
            fingerprint_source(cte.source)
            + "-".join([fingerprint_cte(x, select_condition) for x in cte.parent_ctes])
            + fingerprint_grain(cte.grain)
        )

    if cte.condition:
        base += fingerprint_filter(cte.condition)
    if select_condition:
        base += fingerprint_filter(select_condition)
    return "ds" + base


def process_raw(
    inputs: List[RowsetDerivationStatement | SelectStatement | PersistStatement],
    env: Environment,
    generator: BaseDialect,
    threshold: int = 2,
    inject: bool = False,
) -> tuple[
    list[RowsetDerivationStatement | SelectStatement | PersistStatement],
    list[PersistStatement],
]:
    complete = False
    outputs = []
    x = 0
    while not complete:
        x += 1
        print(f" loop {x}")
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
        return AnalyzeResult({}, {})
    for x in valid:
        for cte in x.ctes:
            # if the CTE is being generated as part of a larger query with a filter condition
            # and it itself doesn't have a filter condition; this might be post filtering
            # and irrelevant to global graph.
            # TODO: figure out if it's pre-filtering or post-filtering
            if x.where_clause and not cte.condition:
                continue
            fingerprint = fingerprint_cte(
                cte, x.where_clause.conditional if x.where_clause else None
            )
            counts[fingerprint] = counts.get(fingerprint, 0) + 1
            lookup[fingerprint] = lookup.get(fingerprint, cte) + cte
    for k, v in counts.items():
        print(
            f"CTE {k} outputs {[x.address for x in lookup[k].output_columns]} is used {v} times"
        )
    counts, mapping = remap_dictionary(counts, CTE_NAMES)
    # for k, v in counts.items():
    #     print(f"CTE {k} is used {v} times")
    final_map: dict[str, CTE] = {}
    for fingerprint, cte in lookup.items():
        final_map[mapping[fingerprint]] = cte
    for k2, v2 in final_map.items():
        print(f"CTE {k2} outputs {[x.address for x in v2.output_columns]}")
    return AnalyzeResult(counts, final_map)


def is_raw_source_cte(cte: CTE) -> bool:
    if not len(cte.source.datasources) == 1:
        return False
    if isinstance(cte.source.datasources[0], Datasource):
        source = cte.source.datasources[0]
        if isinstance(source.address, Address) and source.address.is_query:
            return False
        return True
    return False


def has_anon_concepts(cte: CTE) -> bool:
    if any([x.name.startswith(VIRTUAL_CONCEPT_PREFIX) for x in cte.output_columns]):
        return True
    return False


def reorder_ctes(
    input: list[CTE],
):
    import networkx as nx

    # Create a directed graph
    G: nx.DiGraph = nx.DiGraph()
    mapping: dict[str, CTE] = {}
    for cte in input:
        mapping[cte.name] = cte
        for parent in cte.parent_ctes:
            G.add_edge(parent.name, cte.name)
    # Perform topological sort (only works for DAGs)
    try:
        topological_order = list(nx.topological_sort(G))
        if not topological_order:
            return input
        return [mapping[x] for x in topological_order]
    except nx.NetworkXUnfeasible as e:
        print(
            "The graph is not a DAG (contains cycles) and cannot be topologically sorted."
        )
        raise e


def process_loop(
    inputs: List[
        ProcessedQuery
        | ProcessedQueryPersist
        | ProcessedShowStatement
        | ProcessedRawSQLStatement
    ],
    env: Environment,
    generator: BaseDialect,
    threshold: int = 2,
) -> ProcessLoopResult:
    analysis = analyze(
        [x for x in inputs if isinstance(x, (ProcessedQuery, ProcessedQueryPersist))],
        env,
    )
    new_persist = []
    for k, v in analysis.counts.items():
        print(f"CTE {k} is used {v} times")
        if v >= threshold:
            cte = analysis.mapping[k]
            # skip materializing materialized things
            if is_raw_source_cte(cte):
                continue
            if has_anon_concepts(cte):
                continue
            print(
                f"Creating new persist statement from cte {k} with output {[x.address for x in cte.output_columns]}"
            )
            new_persist.append(cte_to_persist(cte, k, generator=generator))

    return ProcessLoopResult(new=new_persist)
