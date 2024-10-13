from pathlib import Path

from trilogy import Executor
from dotenv import load_dotenv
from datetime import datetime
import pytest
import json

load_dotenv()

working_path = Path(__file__).parent


def run_query(engine: Executor, idx: int, profile: bool = False):

    with open(working_path / f"query{idx:02d}.preql") as f:
        text = f.read()

    comp_text = None
    explicit_sql_comp_path = Path(working_path / f"query{idx:02d}.sql")
    if explicit_sql_comp_path.exists():
        with open(explicit_sql_comp_path) as f:
            comp_text = f.read()
    # warmup - don't count
    base = engine.execute_raw_sql(f"PRAGMA tpcds({idx});")

    start = datetime.now()
    # fetch our results
    base = engine.execute_raw_sql(f"PRAGMA tpcds({idx});")
    comped = datetime.now()
    comp_time = comped - start
    base_results = list(base.fetchall())

    queries = engine.parse_text(text)
    parsed = datetime.now()
    parse_time = datetime.now() - comped

    generated_sql = engine.generate_sql(queries[-1])
    generated = datetime.now()
    generation_time = generated - parsed

    results = engine.execute_raw_sql(generated_sql[-1])

    # reset generated since we're running this twice
    if profile:
        engine.execute_raw_sql("PRAGMA enable_profiling = 'json';")
        engine.execute_raw_sql(
            f"PRAGMA profile_output = '{working_path / 'profiling.json'}';"
        )
    generated = datetime.now()
    results = engine.execute_raw_sql(generated_sql[-1])
    execed = datetime.now()
    if profile:
        return
    exec_time = execed - generated
    comp_results = list(results.fetchall())
    assert len(comp_results) > 0, "No results returned"

    # run the built-in comp

    # check we got it
    if len(base_results) != len(comp_results):
        assert False, f"Row count mismatch: {len(base_results)} != {len(comp_results)}"
    for ridx, row in enumerate(base_results):
        assert (
            row == comp_results[ridx]
        ), f"Row mismatch (expected v actual): {row} != {comp_results[ridx]}"
    if comp_text:
        raw_comp_time = datetime.now()
        comp_results = engine.execute_raw_sql(comp_text)
        raw_comp_duration = datetime.now() - raw_comp_time
    with open(working_path / f"zquery{idx:02d}.log", "w") as f:
        f.write(
            json.dumps(
                {
                    "query_id": idx,
                    "parse_time": (parse_time.total_seconds()),
                    "generation_time": (generation_time.total_seconds()),
                    "exec_time": (exec_time.total_seconds()),
                    "comp_time": comp_time.total_seconds(),
                    "total_time": (datetime.now() - start).total_seconds(),
                    "sql_comp_time": (
                        raw_comp_duration.total_seconds() if comp_text else None
                    ),
                    "generated_sql": generated_sql[-1],
                },
                indent=4,
            )
        )


def test_one(engine):
    run_query(engine, 1)


@pytest.mark.skip(reason="Is duckdb correct??")
def test_two(engine):
    run_query(engine, 2)


def test_three(engine):
    run_query(engine, 3)


@pytest.mark.skip(reason="WIP")
def test_four(engine):
    run_query(engine, 4)


@pytest.mark.skip(reason="WIP")
def test_five(engine):
    run_query(engine, 5)


def test_six(engine):
    run_query(engine, 6)


def test_seven(engine):
    run_query(engine, 7)


def test_eight(engine):
    run_query(engine, 8)


def test_ten(engine):
    run_query(engine, 10)


def test_twelve(engine):
    run_query(engine, 12)


@pytest.mark.skip(reason="WIP")
def test_sixteen(engine):
    run_query(engine, 16)


def run_adhoc(number: int):
    from trilogy import Environment, Dialects
    from trilogy.hooks.query_debugger import DebuggingHook

    env = Environment(working_path=Path(__file__).parent)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook()]
    )
    engine.execute_raw_sql(
        """INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=1);"""
    )
    run_query(engine, number, profile=False)
    print("passed!")


if __name__ == "__main__":
    run_adhoc(6)
