from logging import getLogger
from logging import StreamHandler, INFO

TRILOGY_NAMESPACE = "_trilogyt"

OPTIMIZATION_NAMESPACE = "optimization"


logger = getLogger()

logger.addHandler(StreamHandler())
logger.setLevel(INFO)
