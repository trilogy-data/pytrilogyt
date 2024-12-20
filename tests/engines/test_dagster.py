from pathlib import Path

from trilogyt.dagster.generate import ModelInput


def test_model_input():
    model_input = ModelInput(
        name="test",
        file_path="test",
        import_path=Path(
            "assets\optimization\ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model.py"
        ),
    )
    assert (
        model_input.python_import
        == "assets.optimization.ds445831a9e9e484d7f90c672304367319328b5fd8d8093cc8a12c9d788f4e4bd7_gen_model"
    )
