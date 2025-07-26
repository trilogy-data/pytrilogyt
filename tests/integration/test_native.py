import os
from pathlib import Path

import pytest
from click.testing import CliRunner
from trilogy import Dialects, Environment

from trilogyt.scripts.main import main, native_wrapper

root = Path(__file__)


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
    assert len(output) == 10, [f for f in output]
    for f in output:
        if f.is_file():
            if "customer_" not in f.stem:
                continue
            with open(f) as file:
                number = f.stem.split("_")[-1]
                content = file.read()
                # validate we are using our generated model
                assert "import _opt" in content, content
                # validate we aren't including extra files
                checks = ["one", "two", "three", "four"]
                for check in checks:
                    if check == number:
                        continue
                    assert f"dim_splits_{check}" not in content, content

            exec = Dialects.DUCK_DB.default_executor(
                environment=Environment(working_path=f.parent)
            )
            sql = exec.generate_sql(content)[-1]
            assert "[1,2,3,4]" not in sql, f"SQL should not contain static data: {sql}"


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


def test_file_build_native_folder():
    runner = CliRunner()
    path = Path(__file__).parent / "preql" / "nested"
    result = runner.invoke(
        main,
        [
            "trilogy",
            str(path),
            str(root.parent / "native_multi_file"),
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
