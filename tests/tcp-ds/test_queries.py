from preql import parse
from pathlib import Path

from preql import Environment
from preql import Dialects
from preql.dialect.config import SnowflakeConfig
from os import environ
import snowflake.connector
from preql.core.env_processor import generate_graph
from dotenv import load_dotenv
from datetime import datetime
from preql.hooks.query_debugger import DebuggingHook
import os

load_dotenv()
working_path = Path(__file__).parent
test = working_path / "queries.preql"


def test_one():
    env = Environment(working_path=working_path)

    start = datetime.now()
    with open( working_path / "query01.preql") as f:
        text = f.read()
        env, queries = parse(text, env)


    print(datetime.now()-start)
    exec = Dialects.SNOWFLAKE.default_executor(environment=env, conf=SnowflakeConfig(
        username='EFROMVT',
        password=os.environ['SNOWFLAKE_PW'],
        account = 'DHZVXJH-DE83081',

    ), hooks=[DebuggingHook(process_other=False, process_ctes=False)])
    results = exec.generate_sql(text)
    # results = exec.execute_text(text)
    # for row in results[0].fetchall():
    #     print(row)

def test_two():
    env = Environment(working_path=working_path)

    start = datetime.now()
    with open( working_path / "query02.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    print(datetime.now()-start)
    exec = Dialects.SNOWFLAKE.default_executor(environment=env, conf=SnowflakeConfig(
        username='EFROMVT',
        password=os.environ['SNOWFLAKE_PW'],
        account = 'DHZVXJH-DE83081',

    ), hooks=[DebuggingHook(process_other=False, process_ctes=False)])
    # results = exec.generate_sql(text)
    results = exec.execute_text(text)
    for row in results[0].fetchall():
        print(row)


def test_three():
    env = Environment(working_path=working_path)

    start = datetime.now()
    with open( working_path / "query03.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    print(datetime.now()-start)
    exec = Dialects.SNOWFLAKE.default_executor(environment=env, conf=SnowflakeConfig(
        username='EFROMVT',
        password=os.environ['SNOWFLAKE_PW'],
        account = 'DHZVXJH-DE83081',

    ), hooks=[DebuggingHook(process_other=False, process_ctes=False)])
    # results = exec.generate_sql(text)
    results = exec.execute_text(text)
    for row in results[0].fetchall():
        print(row)

if __name__ == "__main__":
    test_three()
