from pathlib import Path

from trilogy import Dialects, Environment

built_path = Path(__file__).parent / "output"

env = Environment(working_path=built_path)

exec = Dialects.DUCK_DB.default_executor(environment=env)

exec.execute_raw_sql(
    """
    INSTALL tpcds;
    LOAD tpcds;
    SELECT * FROM dsdgen(sf=0.1);
    """
)


run = exec.execute_file(
    file=built_path
    / "_trilogyt_opt_4552edcd331fbbd1d6dcfd95f7b37e1b374b98abbd14455b42a551bdf8625961.preql",
    non_interactive=True,
)

for z in env.datasources:
    print(z)
from trilogy.hooks.query_debugger import DebuggingHook

DebuggingHook()
r = exec.generate_sql(
    """SELECT
    returns.return_amount,
    returns.customer.text_id,
    returns.store.id,
    returns.item.id,
    returns.store_sales.ticket_number,
;"""
)
print("---------")
print(r[-1])
