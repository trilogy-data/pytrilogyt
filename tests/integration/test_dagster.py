import os
from pathlib import Path

import pytest
from click.testing import CliRunner
from trilogy import Dialects

# core\processing\concept_strategies_v3
from trilogy.hooks.query_debugger import DebuggingHook

from trilogyt.scripts.main import dagster_wrapper, main

root = Path(__file__)


def test_full_model_build_dagster(logger):
    DebuggingHook()
    fake = root.parent / "dagster" / "assets" / "customer_two" / "fake_gen_model.py"
    staging_path = root.parent / "preql_dagster_staging/"
    os.makedirs(fake.parent, exist_ok=True)
    os.makedirs(staging_path, exist_ok=True)
    with open(fake, "w") as f:
        f.write("print(1)")
    assert fake.exists()
    dagster_wrapper(
        root.parent / "preql/",
        root.parent / "dagster/",
        Dialects.DUCK_DB,
        run=True,
        debug=False,
        staging_path=root.parent / "preql_dagster_staging/",
    )

    results = root.parent / "dagster/models"
    output = results.glob("**/*.sql")
    for f in output:
        # our generated file
        if "dim_splits" not in str(f):
            continue
        if f.is_file():
            with open(f) as file:
                content = file.read()
                # validate we are using our generated model
                assert "_gen_model')" in content, content

    assert not fake.exists(), f"Fake file {fake} was not deleted"


def test_cli_string_dagster():
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "dagster",
            "persist static_one into static_one from select 1-> test;",
            str(root.parent / "dagster_static/"),
            "duck_db",
            "--run",
        ],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0

    with open(
        root.parent / "dagster" / "assets" / "io" / "static_one_gen_model.py"
    ) as f:
        content = f.read()
        assert "def static_one(duck_db: DuckDBResource)" in content, content


@pytest.mark.skip(reason="Need fixes to get this working in CI")
def test_entrypoint(script_runner):
    result = script_runner.run(
        [
            "trilogyt-test",
            '"""constant x <-5; persist into static as static select x;"""',
            "duckdb",
        ]
    )
    assert result.returncode == 0
