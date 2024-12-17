import os
from datetime import datetime
from pathlib import Path

import pytest
from dotenv import load_dotenv
from trilogy import Dialects, Environment, parse
from trilogy.dialect.config import SnowflakeConfig
from trilogy.hooks.query_debugger import DebuggingHook

load_dotenv()
working_path = Path(__file__).parent
test = working_path / "queries.preql"

RUN_SNOWFLAKE = os.getenv("RUN_SNOWFLAKE", False)


def run_snowflake(env: Environment, text: str):
    exec = Dialects.SNOWFLAKE.default_executor(
        environment=env,
        conf=SnowflakeConfig(
            username="EFROMVT",
            password=os.environ["SNOWFLAKE_PW"],
            account=os.environ["SNOWFLAKE_ENV"],
        ),
        hooks=[DebuggingHook(process_other=False, process_ctes=False)],
    )
    results = exec.execute_text(text)
    for row in results[0].fetchall():
        print(row)


def render_duck_db(env: Environment, text: str):
    exec = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[DebuggingHook(process_other=False, process_ctes=False)],
    )
    _ = exec.generate_sql(text)


@pytest.mark.skip(reason="no billing")
def test_one():
    env = Environment(working_path=working_path)

    start = datetime.now()
    with open(working_path / "query01.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    print(datetime.now() - start)
    if RUN_SNOWFLAKE:
        run_snowflake(env, text)
    else:
        render_duck_db(env, text)


@pytest.mark.skip(reason="no billing")
def test_two():
    env = Environment(working_path=working_path)

    start = datetime.now()
    with open(working_path / "query02.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    print(datetime.now() - start)
    if RUN_SNOWFLAKE:
        run_snowflake(env, text)
    else:
        render_duck_db(env, text)


@pytest.mark.skip(reason="no billing")
def test_three():
    env = Environment(working_path=working_path)

    start = datetime.now()
    with open(working_path / "query03.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    print(datetime.now() - start)
    if RUN_SNOWFLAKE:
        run_snowflake(env, text)
    else:
        render_duck_db(env, text)
