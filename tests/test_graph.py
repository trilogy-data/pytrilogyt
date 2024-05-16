from pypreqlt.graph import fingerprint_cte
from preql.core.models import CompiledCTE, ProcessedQuery, CTE


def test_graph():
    
    items = fingerprint_cte(CTE(name="test", statement="select 1"))