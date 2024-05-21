from dbt.cli.main import dbtRunner, dbtRunnerResult, RunExecutionResult
from pathlib import Path


def run_path(path: Path):
    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    cli_args = [
        "run",
        "--project-dir",
        str(path),
        "--profiles-dir",
        str(path / ".dbt"),
    ]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)
    if not res.success:
        raise RuntimeError(res.exception)
    # inspect the results
    if not isinstance(res.result, RunExecutionResult):
        return
    res_output: RunExecutionResult = res.result
    for r in res_output:
        if not r:
            continue
        print(f"{r.node.name}: {r.status}")
