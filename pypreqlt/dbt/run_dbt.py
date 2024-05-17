from dbt.cli.main import dbtRunner, dbtRunnerResult
from pathlib import Path


def run_path(path: Path):
    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    cli_args = ["run", "--project-dir", path, " --profiles-dir", path / ".dbt"]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # inspect the results
    for r in res.result:
        print(f"{r.node.name}: {r.status}")
