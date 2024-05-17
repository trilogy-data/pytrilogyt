from pypreqlt.graph import fingerprint_cte
from preql.core.models import CTE


def test_graph():
    fingerprint_cte(CTE(name="test", statement="select 1"))
