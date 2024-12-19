from logging import INFO, ERROR

from pytest import fixture

from trilogyt.constants import logger as root_logger
from trilogy.constants import logger as trilogy_logger

@fixture(scope="session")
def logger():
    # handler = StreamHandler()
    # handler.setLevel(level=INFO)
    # root_logger.addHandler(handler)
    trilogy_logger.setLevel(level=ERROR)
    trilogy_logger.handlers = []
    root_logger.setLevel(level=INFO)
    yield root_logger
