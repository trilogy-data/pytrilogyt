import os
from pathlib import Path

import pytest
from click.testing import CliRunner
from trilogy import Dialects
from trilogy.hooks.query_debugger import DebuggingHook

from trilogyt.scripts.main import dbt_wrapper, main

root = Path(__file__)


def test_full_model_build_dbt(logger):
    DebuggingHook()
    fake = root.parent / "dbt" / "models" / "customer_two" / "fake_gen_model.sql"
    staging_path = root.parent / "preql_dbt_staging/"
    os.makedirs(fake.parent, exist_ok=True)
    os.makedirs(staging_path, exist_ok=True)
    with open(fake, "w") as f:
        f.write("SELECT 1")
    assert fake.exists()
    dbt_wrapper(
        root.parent / "preql/",
        root.parent / "dbt/",
        Dialects.DUCK_DB,
        run=True,
        debug=False,
        staging_path=root.parent / "preql_dbt_staging/",
    )

    results = root.parent / "dbt/models"
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


def test_cli_string_dbt():
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "dbt",
            "persist static_one into static_one from select 1-> test;",
            str(root.parent / "dbt/"),
            "duck_db",
            "--run",
        ],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0


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
