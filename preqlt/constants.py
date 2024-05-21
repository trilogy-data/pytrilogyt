from logging import getLogger
from logging import StreamHandler, INFO

PREQLT_NAMESPACE = "_preqlt"

OPTIMIZATION_NAMESPACE = "optimization"


logger = getLogger()

logger.addHandler(StreamHandler())
logger.setLevel(INFO)
