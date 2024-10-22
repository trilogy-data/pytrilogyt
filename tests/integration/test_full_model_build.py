from trilogyt.scripts.main import dbt_wrapper, main, native_wrapper
from trilogy import Dialects
from pathlib import Path
from click.testing import CliRunner
import pytest
import os

root = Path(__file__)


def test_full_model_build_dbt(logger):
    fake = root.parent / "dbt" / "models" / "customer_two" / "fake_gen_model.sql"
    with open(fake, "w") as f:
        f.write("SELECT 1")
    assert fake.exists()
    dbt_wrapper(
        root.parent / "preql/",
        root.parent / "dbt/",
        Dialects.DUCK_DB,
        run=True,
        debug=False,
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


def test_full_model_build_native(logger):
    fake = root.parent / "native"
    os.makedirs(fake, exist_ok=True)
    assert fake.exists()
    native_wrapper(
        root.parent / "preql/",
        root.parent / "native/",
        Dialects.DUCK_DB,
        run=True,
        debug=False,
    )

    results = root.parent / "native"
    output = list(results.glob("**/*.preql"))
    assert len(output) == 7, [f for f in output]
    for f in output:
        # our generated file
        if "dim_splits" not in str(f):
            continue
        if f.is_file():
            with open(f) as file:
                content = file.read()
                # validate we are using our generated model
                assert "import optimize" in content, content


def test_cli_string_dbt():
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "dbt",
            "persist static_one into static_one from select 1-> test;",
            str(root.parent / "dbt/"),
            "duck_db",
        ],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0


def test_cli_string_native():
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "trilogy",
            "persist static_one into static_one from select 1-> test;",
            str(root.parent / "native/"),
            "duck_db",
        ],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0


def test_file_build_native():
    runner = CliRunner()
    path = Path(__file__).parent / "preql" / "customer_one.preql"
    result = runner.invoke(
        main,
        [
            "trilogy",
            str(path),
            str(root.parent / "native_single_file"),
            "duck_db",
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
