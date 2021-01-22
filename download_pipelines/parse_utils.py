import csv
import json
import sys

from download_pipelines.logging_utils import set_logger
from download_pipelines.xml_utils import xml_findall_deep

logger = set_logger(__name__)


def parse_json(filename: str):
    logger.debug("Parsing as JSON: %s" % filename)
    with open(filename, "r") as f:
        contents = f.read()
    return json.loads(contents)


def parse_xml(filename, tag=None):
    logger.debug("Parsing as XML and looking for tag <%s>: %s" %
                 (tag, filename))
    return xml_findall_deep(filename, tag)


def parse_csv(filename, delimiter: str = "\t"):
    logger.debug("Parsing as CSV with delimiter '%s': %s" %
                 (delimiter, filename))
    csv.field_size_limit(sys.maxsize)
    with open(filename, "r") as f:
        contents = f.read()
    return list(csv.DictReader(contents.split("\n"), delimiter=delimiter))
