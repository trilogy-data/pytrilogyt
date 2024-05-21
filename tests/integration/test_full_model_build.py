from preqlt.scripts.main import main_file_wrapper, main
from preql import Dialects
from pathlib import Path
from click.testing import CliRunner

root = Path(__file__)
# preql: str | Path, dbt_path:Path, dialect: str, debug: bool, run: bool):


def test_full_model_build():

    main_file_wrapper(
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
        if "optimization" in str(f):
            continue
        if f.is_file():
            with open(f) as file:
                content = file.read()
                # validate we are using our generated model
                assert "ref('split_gen_model')" in content


def test_cli_string():
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "persist static_one into static_one from select 1-> test;",
            str(root.parent / "dbt/"),
            "duck_db",
        ],
    )
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
