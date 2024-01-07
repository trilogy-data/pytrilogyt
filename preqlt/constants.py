from logging import getLogger
from logging import StreamHandler, INFO

logger = getLogger()

logger.addHandler(StreamHandler())
logger.setLevel(INFO)