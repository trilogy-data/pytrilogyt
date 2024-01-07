from dbt.cli.main import dbtRunner, dbtRunnerResult


def run_path(path):
    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    cli_args = ["run", "--project-dir", path ]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # inspect the results
    for r in res.result:
        print(f"{r.node.name}: {r.status}")

