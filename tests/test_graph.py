from preqlt.graph import fingerprint_cte, process_raw
from preql.core.models import CTE
from preql import Dialects, Environment, parse
from preql.dialect.duckdb import DuckDBDialect
from preql.parsing.render import Renderer

def test_graph():
    fingerprint_cte(CTE(name="test", statement="select 1"))


def test_integration():
    env = Environment()

    parsed = parse('''
const int_array<- [1,2,3,4];
                   
auto split <- unnest(int_array);
                   
select split;

select split;

select split;      

''', environment=env)
    exec = Dialects.DUCK_DB.default_executor(environment=env)
    dialect = DuckDBDialect()
    consolidated = process_raw(parsed, env=env, dialect =dialect(), threshold=2)

    # we should have the one consolidated CTE first
    assert len(consolidated) == 4
    renderer = Renderer()
    final = []
    for x in consolidated:
        final.append(renderer.to_string(x))

    raise ValueError(';\n'.join(final))