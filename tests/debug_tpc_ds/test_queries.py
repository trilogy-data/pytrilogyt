from datetime import datetime
from pathlib import Path

import tomli_w
from dotenv import load_dotenv
from trilogy import Environment, Executor
from trilogy.hooks.query_debugger import DebuggingHook

load_dotenv()

working_path = Path(__file__).parent


def run_test_case(
    path: Path,
    optimized_path: Path,
    idx: int,
    test_engine: Executor,
    base_engine: Executor,
    base_results,
):
    output_timers = {}
    for engine, label, preql_path in [
        [base_engine, "base", working_path / f"query{idx:02d}.preql"],
        [test_engine, "optimized", optimized_path / f"query{idx:02d}_optimized.preql"],
    ]:
        with open(preql_path) as f:
            text = f.read()
        prep_time = datetime.now()
        # engine.environment = Environment(working_path=preql_path.parent)

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
            ), f"Row count mismatch for {label}: {len(base_results)} != {len(comp_results)}"
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
    engine: Executor,
    base_engine: Executor,
    idx: int,
    optimized_path,
    profile: bool = False,
):

    start = datetime.now()
    base = base_engine.execute_raw_sql(f"PRAGMA tpcds({idx});")
    comped = datetime.now()
    comp_time = comped - start
    base_results = list(base.fetchall())

    test_results = run_test_case(
        working_path, optimized_path, idx, engine, base_engine, base_results
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


def test_one(engine, base_engine, optimized_env):
    run_query(engine, base_engine, 1, optimized_env)
    assert 1 == 1


def test_three(engine, base_engine, optimized_env):
    DebuggingHook()
    run_query(engine, base_engine, 3, optimized_env)
    assert 1 == 1


def test_three(engine, base_engine, optimized_env):
    DebuggingHook()
    run_query(engine, base_engine, 3, optimized_env)
    assert 1 == 1


def test_six(engine, base_engine, optimized_env):
    DebuggingHook()
    run_query(engine, base_engine, 6, optimized_env)
    assert 1 == 1


def test_seven(engine, base_engine, optimized_env):
    DebuggingHook()
    run_query(engine, base_engine, 7, optimized_env)
    assert 1 == 1


def test_eight(engine, base_engine, optimized_env):
    DebuggingHook()
    run_query(engine, base_engine, 8, optimized_env)
    assert 1 == 1


def run_adhoc_compiled(number: int):
    from trilogy import Dialects
    from trilogy.hooks.query_debugger import DebuggingHook

    parent = Path(__file__).parent
    env = Environment(working_path=Path(__file__).parent)
    engine: Executor = Dialects.DUCK_DB.default_executor(
        environment=env, hooks=[DebuggingHook()]
    )
    optimized_engine = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[DebuggingHook()],
        conf=engine.conf,
    )
    engine.execute_raw_sql(
        """INSTALL tpcds;
LOAD tpcds;
SELECT * FROM dsdgen(sf=.1);"""
    )
    run_query(engine, number, optimized_path=parent / "output", profile=False)
    print("passed!")


if __name__ == "__main__":
    run_adhoc_compiled(1)
