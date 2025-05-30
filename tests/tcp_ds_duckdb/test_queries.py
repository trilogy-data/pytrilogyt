from datetime import datetime
from pathlib import Path

import pytest
import tomli_w
from dotenv import load_dotenv
from trilogy import Environment, Executor

from tests.tcp_ds_duckdb.conftest import OptimizationResult, OptimizedEnv

load_dotenv()

working_path = Path(__file__).parent


def run_test_case(
    path: Path,
    optimized_path: Path,
    idx: int,
    engine: Executor,
    base_results,
    mappings: dict[Path, OptimizationResult],
):
    output_timers = {}
    for label, preql_path in [
        ["base", working_path / f"query{idx:02d}.preql"],
        ["optimized", optimized_path / f"query{idx:02d}.preql"],
    ]:
        with open(preql_path) as f:
            text = f.read()
        prep_time = datetime.now()
        engine.environment = Environment(working_path=preql_path.parent)
        if label == "optimized":
            engine.execute_file(
                mappings[working_path / "query01.preql"].path, non_interactive=True
            )
        end_prep = datetime.now()
        parsed = datetime.now()

        queries = engine.parse_text(text)
        post_parsed = datetime.now()
        generated_sql = engine.generate_sql(queries[-1])
        generated = datetime.now()
        results = engine.execute_raw_sql(generated_sql[-1])
        execed = datetime.now()
        exec_time = execed - generated
        comp_results = list(results.fetchall())
        assert len(comp_results) > 0, "No results returned"
        # check we got it
        if len(base_results) != len(comp_results):
            assert (
                False
            ), f"Row count mismatch: {len(base_results)} != {len(comp_results)}"
        for ridx, row in enumerate(base_results):
            assert row == comp_results[ridx], (
                f"{label} row mismatch (expected v actual) for row {ridx} : {row} != {comp_results[ridx]}"
                + "\nquery:"
                + generated_sql[-1]
            )
        output_timers[f"{label}_prep_time"] = (end_prep - prep_time).total_seconds()
        output_timers[f"{label}_parse_time"] = (post_parsed - parsed).total_seconds()
        output_timers[f"{label}_generation_time"] = (
            generated - post_parsed
        ).total_seconds()
        output_timers[f"{label}_exec_time"] = exec_time.total_seconds()
        output_timers[f"{label}_generated_sql"] = generated_sql[-1]
    return output_timers


def run_query(
    engine: Executor, idx: int, optimized_env: OptimizedEnv, profile: bool = False
):

    optimized_path = optimized_env.temp_dir

    start = datetime.now()
    base = engine.execute_raw_sql(f"PRAGMA tpcds({idx});")
    comped = datetime.now()
    comp_time = comped - start
    base_results = list(base.fetchall())

    test_results = run_test_case(
        working_path, optimized_path, idx, engine, base_results, optimized_env.mappings
    )
    base_output = {
        "query_id": idx,
        "sql_comp_time": comp_time.total_seconds(),
        "total_time": (datetime.now() - start).total_seconds(),
    }
    test_results.update(base_output)
    with open(working_path / f"zquery{idx:02d}.log", "w") as f:
        f.write(
            tomli_w.dumps(
                test_results,
                multiline_strings=True,
            )
        )

from trilogy.parsing.parse_engine import ParsePass
def test_one(engine, optimized_env):
    run_query(engine, 1, optimized_env)


@pytest.mark.skip(reason="Is duckdb correct??")
def test_two(engine, optimized_env):
    run_query(engine, 2, optimized_env)


def test_three(engine, optimized_env):
    run_query(engine, 3, optimized_env)


@pytest.mark.skip(reason="WIP")
def test_four(engine, optimized_env):
    run_query(engine, 4, optimized_env)


@pytest.mark.skip(reason="WIP")
def test_five(engine, optimized_env):
    run_query(engine, 5, optimized_env)


def test_six(engine, optimized_env):
    run_query(engine, 6, optimized_env)


def test_seven(engine, optimized_env):
    run_query(engine, 7, optimized_env)


def test_eight(engine, optimized_env):
    run_query(engine, 8, optimized_env)


def test_ten(engine, optimized_env):
    run_query(engine, 10, optimized_env)


def test_twelve(engine, optimized_env):
    run_query(engine, 12, optimized_env)


@pytest.mark.skip(reason="WIP")
def test_sixteen(engine):
    run_query(engine, 16)


# def run_adhoc(number: int):
#     from trilogy import Environment, Dialects
#     from trilogy.hooks.query_debugger import DebuggingHook

#     env = Environment(working_path=Path(__file__).parent)
#     engine: Executor = Dialects.DUCK_DB.default_executor(
#         environment=env, hooks=[DebuggingHook()]
#     )
#     engine.execute_raw_sql(
#         """INSTALL tpcds;
# LOAD tpcds;
# SELECT * FROM dsdgen(sf=1);"""
#     )
#     run_query(engine, number, profile=False)
#     print("passed!")


def run_adhoc_compiled(number: int):
    from trilogy import Dialects, Environment
    from trilogy.hooks.query_debugger import DebuggingHook

    parent = Path(__file__).parent
    env = Environment(working_path=Path(__file__).parent)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook()]
    )
    engine.execute_raw_sql(
        """INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=1);"""
    )
    run_query(engine, number, optimized_path=parent / "preql_staging", profile=False)
    print("passed!")


if __name__ == "__main__":
    run_adhoc_compiled(1)
