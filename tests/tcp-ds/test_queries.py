from preql import parse
from pathlib import Path

from preql import Environment
from preql import Dialects
from dotenv import load_dotenv
from datetime import datetime
from preql.hooks.query_debugger import DebuggingHook

load_dotenv()
working_path = Path(__file__).parent
test = working_path / "queries.preql"


def test_one():
    env = Environment(working_path=working_path)

    start = datetime.now()
    with open(working_path / "query01.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    print(datetime.now() - start)
    exec = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[DebuggingHook(process_other=False, process_ctes=False)],
    )
    exec.generate_sql(text)
    # results = exec.execute_text(text)
    # for row in results[0].fetchall():
    #     print(row)


def test_two():
    env = Environment(working_path=working_path)

    start = datetime.now()
    with open(working_path / "query02.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    print(datetime.now() - start)
    exec = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[DebuggingHook(process_other=False, process_ctes=False)],
    )
    results = exec.generate_sql(text)
    # results = exec.execute_text(text)
    # for row in results[0].fetchall():
    #     print(row)


def test_three():
    env = Environment(working_path=working_path)

    start = datetime.now()
    with open(working_path / "query03.preql") as f:
        text = f.read()
        env, queries = parse(text, env)

    print(datetime.now() - start)
    exec = Dialects.DUCK_DB.default_executor(
        environment=env,
        hooks=[DebuggingHook(process_other=False, process_ctes=False)],
    )
    # exec = Dialects.SNOWFLAKE.default_executor(
    #     environment=env,
    #     conf=SnowflakeConfig(
    #         username="EFROMVT",
    #         password=os.environ["SNOWFLAKE_PW"],
    #         account=os.environ["SNOWFLAKE_ENV"],
    #     ),
    #     hooks=[DebuggingHook(process_other=False, process_ctes=False)],
    # )
    results = exec.generate_sql(text)
    # results = exec.execute_text(text)
    # for row in results[0].fetchall():
    #     print(row)


if __name__ == "__main__":
    test_three()
