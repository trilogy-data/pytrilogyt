from logging import getLogger
from logging import StreamHandler, INFO

PREQLT_NAMESPACE = "_preqlt"


logger = getLogger()

logger.addHandler(StreamHandler())
logger.setLevel(INFO)
