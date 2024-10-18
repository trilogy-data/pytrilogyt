from pathlib import Path
from trilogy import Dialects, Environment
from trilogyt.constants import logger


def run_path(path: Path, dialect: Dialects):
    # initialize

    for x in path.glob("**/*.preql"):
        try:
            env = Environment(working_path=path)
            executor = dialect.default_executor(environment=env)
            executor.execute_file(x)
        except Exception as e:
            logger.error(f" Error executing {x} {e}")
            raise e
