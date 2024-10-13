from trilogyt.graph import fingerprint_cte, process_raw
from trilogy.core.models import CTE, QueryDatasource, Environment, LooseConceptList
from trilogy import Dialects, parse
from trilogy.dialect.duckdb import DuckDBDialect
from trilogy.parsing.render import Renderer
from trilogy.core.enums import SourceType


def test_fingerprint(test_environment: Environment):

    oid = test_environment.concepts["order_id"]
    oid_ds = test_environment.datasources["orders"]
    qds = QueryDatasource(
        output_concepts=[oid],
        input_concepts=[oid],
        datasources=[oid_ds],
        source_map={oid.address: {oid_ds}},
        grain=oid_ds.grain,
        source_type=SourceType.DIRECT_SELECT,
        joins=[],
    )
    test = CTE(
        name="test",
        source=qds,
        output_columns=[oid],
        source_map={oid.address: [qds.name]},
        grain=qds.grain,
    )
    a = fingerprint_cte(test)
    test.name = "test2"
    b = fingerprint_cte(test)
    assert a == b


def test_integration():
    env = Environment()

    env, parsed = parse(
        """
key int_array list<int>;

datasource int_source (
int_array:int_array
)
grain (int_array)
query '''
select [1,2,3,4] as int_array
''';
                   
auto split <- unnest(int_array);
                   
select split;

select split;

select split;      

""",
        environment=env,
    )
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    dialect = DuckDBDialect()
    initial = dialect.generate_queries(env, parsed)
    assert len(initial) == 3
    consolidated, new = process_raw(
        parsed, env=env, generator=dialect, threshold=2, inject=True
    )

    # we should have the one consolidated CTE first
    assert len(consolidated) == 8
    renderer = Renderer()
    final = []
    for x in consolidated:
        final.append(renderer.to_string(x))
    reparsed = exec.parse_text("\n".join(final), persist=True)

    # we should have our new datasource
    assert len(env.datasources) == 3
    env = exec.environment
    split = env.concepts["split"]
    instance = [
        x for x in list(env.datasources.values()) if split.address in x.output_concepts
    ][0]
    assert split.address in [x.address for x in env.materialized_concepts]
    assert "local.split" in [x.address for x in env.materialized_concepts]
    materialized_lcl = LooseConceptList(
        concepts=[
            x
            for x in reparsed[-1].output_columns
            if x.address in [z.address for z in env.materialized_concepts]
        ]
    )
    assert materialized_lcl.addresses == {"local.split"}
    final = reparsed[-1]
    # check that our queries use the new datasource
    assert final.ctes[0].source.datasources[0] == instance, final.ctes[
        0
    ].source.datasources[0]
