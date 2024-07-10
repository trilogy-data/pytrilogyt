from pathlib import Path

from trilogy import Executor
from dotenv import load_dotenv
from datetime import datetime
import pytest
import json

load_dotenv()

working_path = Path(__file__).parent


def run_query(engine: Executor, idx: int):

    with open(working_path / f"query{idx:02d}.preql") as f:
        text = f.read()
    start = datetime.now()
    # fetch our results
    base = engine.execute_raw_sql(f"PRAGMA tpcds({idx});")
    base_results = list(base.fetchall())
    comped = datetime.now()
    comp_time = comped - start

    queries = engine.parse_text(text)
    parsed = datetime.now()
    parse_time = datetime.now() - comped

    generated_sql = engine.generate_sql(queries[-1])
    generated = datetime.now()
    generation_time = generated - parsed
    results = engine.execute_raw_sql(generated_sql[-1])
    comp_results = list(results.fetchall())
    assert len(comp_results) > 0, "No results returned"
    execed = datetime.now()
    exec_time = execed - generated

    # run the built-in comp

    # check we got it
    if len(base_results) != len(comp_results):
        assert False, f"Row count mismatch: {len(base_results)} != {len(comp_results)}"
    for ridx, row in enumerate(base_results):
        assert row == comp_results[ridx]

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
