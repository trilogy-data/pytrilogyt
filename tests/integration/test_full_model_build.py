from trilogyt.scripts.main import main_file_wrapper, main
from trilogy import Dialects
from pathlib import Path
from click.testing import CliRunner

root = Path(__file__)


def test_full_model_build():
    fake = root.parent / "dbt" / "models" / "customer_two" / "fake_gen_model.sql"
    with open(fake, "w") as f:
        f.write("SELECT 1")
    assert fake.exists()
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
        if "dim_splits" not in str(f):
            continue
        if f.is_file():
            with open(f) as file:
                content = file.read()
                # validate we are using our generated model
                assert "_gen_model')" in content

    assert not fake.exists(), f"Fake file {fake} was not deleted"


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
