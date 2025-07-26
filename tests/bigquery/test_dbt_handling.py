from trilogy import Dialects
from trilogy.authoring import Address, Datasource
from trilogy.core.models.execute import CTE
from trilogy.core.statements.execute import ProcessedQuery

from trilogyt.dbt.generate import add_cte_reference


def test_quote_rendering():
    base = Dialects.DUCK_DB.default_executor()

    queries = base.parse_text(
        """
key x int;
property x.val float;

datasource test_source(
    x,
    val
    )
grain (x)
address test_source;

select x;
"""
    )

    target = queries[-1]
    assert isinstance(target, ProcessedQuery)
    for cte in target.ctes:
        if not isinstance(cte, CTE):
            continue
        add_cte_reference(
            cte,
            {
                "test_source": Datasource(
                    name="test_source",
                    # identifier='test_source',
                    columns=[],
                    address=Address(location="test_source", is_query=False),
                )
            },
        )
        # assert not cte.is_root_datasource, cte.source.datasources[0].address
        assert not cte.quote_address, cte.source.datasources[0].address
    generated = base.generate_sql(target)
    assert " {{ ref(" in generated[-1], generated[-1]
