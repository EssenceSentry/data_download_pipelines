import logging
import sys
from datetime import datetime

class TimeFilter(logging.Filter):
    def filter(self, record):
        try:
            last = self.last
        except AttributeError:
            last = record.relativeCreated
        delta = datetime.fromtimestamp(
            record.relativeCreated) - datetime.fromtimestamp(last)
        record.relative = "{0:.2f}ms".format(delta.seconds +
                                             delta.microseconds / 1000000.0)
        self.last = record.relativeCreated
        return True


def set_logger(name=None, logger=None):
    if not logger:
        logging.basicConfig(
            stream=sys.stdout,
            level=logging.DEBUG,
            format=
            "%(asctime)s.%(msecs)03d (%(relative)s) %(levelname)s\t%(message)s",
            datefmt="%H:%M:%S")
        logger = logging.getLogger(name=name)
    logger.addFilter(TimeFilter())
    return logger
