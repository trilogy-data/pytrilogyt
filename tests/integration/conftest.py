from pytest import fixture
from logging import StreamHandler, INFO
from trilogyt.constants import logger as root_logger

@fixture(scope="session")
def logger():
    # handler = StreamHandler()
    # handler.setLevel(level=INFO)
    # root_logger.addHandler(handler)
    root_logger.setLevel(level=INFO)
    yield root_logger