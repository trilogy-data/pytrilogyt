from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from random import choice
from typing import Any, List

from trilogy import Environment
from trilogy.authoring import (
    Address,
    Comparison,
    Concept,
    ConceptRef,
    Conditional,
    Datasource,
    Function,
    Grain,
    Parenthetical,
    PersistStatement,
    RowsetDerivationStatement,
    SelectItem,
    SelectStatement,
    SubselectComparison,
    WhereClause,
)
from trilogy.constants import DEFAULT_NAMESPACE, VIRTUAL_CONCEPT_PREFIX, MagicConstants
from trilogy.core.enums import Derivation
from trilogy.core.ergonomics import CTE_NAMES
from trilogy.core.models.build import (
    BuildComparison,
    BuildConcept,
    BuildConditional,
    BuildDatasource,
    BuildFunction,
    BuildGrain,
    BuildParenthetical,
    BuildSubselectComparison,
)
from trilogy.core.models.core import ListWrapper, MapWrapper, TupleWrapper
from trilogy.core.models.execute import CTE, QueryDatasource, UnionCTE
from trilogy.core.statements.execute import (
    ProcessedQuery,
    ProcessedQueryPersist,
    ProcessedRawSQLStatement,
    ProcessedShowStatement,
)
from trilogy.dialect.base import BaseDialect

from trilogyt.constants import logger


@dataclass
class AnalyzeResult:
    counts: dict[str, int]
    mapping: dict[str, CTE]


@dataclass
class ProcessLoopResult:
    new: List[PersistStatement]


def name_to_short_name(x: str):
    base = x
    if x.startswith(VIRTUAL_CONCEPT_PREFIX):
        base = "virtual"
    return base.replace(".", "_")


# hash a
def hash_concepts(concepts: list[ConceptRef]) -> str:
    return sha256("".join(sorted([x.address for x in concepts])).encode()).hexdigest()


def generate_datasource_name(
    select: SelectStatement, cte_name: str, env: Environment
) -> str:
    """Generate a reasonable table name"""
    grain = select.calculate_grain(env, select.local_concepts)
    if grain.components:
        human = [name_to_short_name(x) for x in grain.components]
    else:
        human = [name_to_short_name(x.name) for x in select.output_components]
    human = sorted(human)
    hash = hash_concepts(select.output_components)
    cutoff = 5
    inputs = human[0:cutoff]
    if len(human) > cutoff:
        inputs.append(f"{len(human) - cutoff}_more")
    base = "ds" + "_".join(inputs + [hash[0:8]])
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


def convert_build_to_author(input: Any):
    if isinstance(input, BuildConcept):
        return ConceptRef(address=input.address)
    if isinstance(input, BuildConditional):
        return Conditional(
            left=convert_build_to_author(input.left),
            right=convert_build_to_author(input.right),
            operator=input.operator,
        )
    elif isinstance(input, BuildComparison):
        return Comparison(
            left=convert_build_to_author(input.left),
            right=convert_build_to_author(input.right),
            operator=input.operator,
        )
    elif isinstance(input, BuildSubselectComparison):
        return SubselectComparison(
            left=convert_build_to_author(input.left),
            right=convert_build_to_author(input.right),
            operator=input.operator,
        )
    elif isinstance(input, BuildParenthetical):
        return Parenthetical(content=convert_build_to_author(input.content))
    elif isinstance(input, BuildFunction):
        return Function(
            operator=input.operator,
            arguments=[convert_build_to_author(x) for x in input.arguments],
            arg_count=input.arg_count,
            output_datatype=input.output_datatype,
            output_purpose=input.output_purpose,
        )
    elif isinstance(
        input,
        (
            int,
            str,
            float,
            bool,
            MagicConstants,
            datetime,
            date,
            TupleWrapper,
            MapWrapper,
            ListWrapper,
        ),
    ):
        return input
    else:
        raise ValueError(f"Unsupported round trip for {type(input)}")


def cte_to_persist(
    input: CTE, name: str, generator: BaseDialect, env: Environment
) -> PersistStatement:

    # a rowset will have the CTE ported over and we can assume the select statement doesn't need
    # further filtering
    rowset = any([x.derivation == Derivation.ROWSET for x in input.output_columns])
    if rowset:
        select = SelectStatement(
            selection=[
                SelectItem(content=ConceptRef(address=x.address))
                for x in sorted(input.output_columns, key=lambda x: x.address)
            ],
        )
    else:
        select = SelectStatement(
            selection=[
                SelectItem(content=ConceptRef(address=x.address))
                for x in sorted(input.output_columns, key=lambda x: x.address)
            ],
            where_clause=(
                WhereClause(conditional=convert_build_to_author(input.condition))
                if input.condition
                else None
            ),
        )
    name = generate_datasource_name(select, name, env)
    datasource = select.to_datasource(
        namespace=DEFAULT_NAMESPACE,
        name=name,
        address=Address(
            location=name,
        ),
        environment=env,
    )

    persist = PersistStatement(datasource=datasource, select=select)
    return persist


def remap_dictionary(d: dict, remap: list[str]) -> tuple[dict, dict]:
    new: dict[str, str] = {}
    mapping: dict[str, str] = {}
    for k, v in d.items():
        alias = None
        while alias is None or alias in new:
            alias = choice(remap)
        new[alias] = v
        mapping[k] = alias
    return new, mapping


def fingerprint_concept(concept: Concept) -> str:
    return concept.address


def fingerprint_grain(grain: Grain | BuildGrain) -> str:
    return "-".join([x for x in grain.components])


def fingerprint_source(source: QueryDatasource | BuildDatasource) -> str:
    if isinstance(source, BuildDatasource):
        return str(source.address)
    return "-".join([fingerprint_source(s) for s in source.datasources]) + str(
        source.limit
    )


def fingerprint_filter(
    filter: (
        Conditional
        | Comparison
        | Parenthetical
        | BuildConditional
        | BuildComparison
        | BuildParenthetical
    ),
) -> str:
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
            fingerprint = fingerprint_cte(cte, cte.condition if cte.condition else None)
            counts[fingerprint] = counts.get(fingerprint, 0) + 1
            lookup[fingerprint] = lookup.get(fingerprint, cte) + cte
    for k, v in counts.items():
        logger.info(
            f"CTE {k} outputs {[x.address for x in lookup[k].output_columns]} is used {v} times"
        )
    counts, mapping = remap_dictionary(counts, CTE_NAMES)

    final_map: dict[str, CTE] = {}
    for fingerprint, cte in lookup.items():
        final_map[mapping[fingerprint]] = cte
    for k2, v2 in final_map.items():
        logger.info(f"CTE {k2} outputs {[x.address for x in v2.output_columns]}")
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
    if cte.condition and any(
        [
            x.name.startswith(VIRTUAL_CONCEPT_PREFIX)
            for x in cte.condition.concept_arguments
        ]
    ):
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
        logger.error(
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
        logger.info(f"CTE {k} is used {v} times")
        if v >= threshold:
            cte = analysis.mapping[k]
            # skip materializing materialized things
            if is_raw_source_cte(cte):
                continue
            if has_anon_concepts(cte):
                continue
            new_persist_stmt = cte_to_persist(cte, k, generator=generator, env=env)
            logger.info(
                f"Creating new persist statement from cte {k} to {new_persist_stmt.datasource.address} with output {[x.address for x in cte.output_columns]} grain {cte.grain.components} conditions {cte.condition}"
            )
            new_persist.append(new_persist_stmt)

    return ProcessLoopResult(new=new_persist)
