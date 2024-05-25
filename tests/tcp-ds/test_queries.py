from preql import parse
from pathlib import Path

from preql import Environment

working_path = Path(__file__).parent
test = working_path / "queries.preql"


def test_one():
    env = Environment(working_path=working_path)
    with open(test) as f:
        env, queries = parse(f.read(), env)


if __name__ == "__main__":
    test_one()
