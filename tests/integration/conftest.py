from pytest import fixture

from trilogy import Environment
from trilogy.core.env_processor import generate_graph
from trilogy import parse, Dialects
from trilogy.hooks.query_debugger import DebuggingHook
from logging import INFO


@fixture(scope="session")
def logger():
    DebuggingHook()