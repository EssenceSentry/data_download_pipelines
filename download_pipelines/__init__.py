"""
This module includes a set of utilities to help in the writing of the data download scripts.

Try reading the documentation for each function.

For any suggestions, do not hesitate to contact me at <sellanes.jose@gmail.com>.
"""

__author__ = "Agustín Sellanes <sellanes.jose@gmail.com>"
__all__ = [
    "capitalize", "concat", "config_from_dynamo", "contents", "date_from_str",
    "download", "FTPConnection", "get", "join", "join_if_different_ids",
    "maybe", "mysql_query", "p_filter", "p_map", "p_print", "p_reduce",
    "parse_csv", "parse_json", "parse_xml", "Pipe", "progress_bar",
    "set_inter", "set_logger", "set_union", "split", "SSHConnection",
    "str_from_date", "strip", "to_set", "ungzip", "untar", "unzip",
    "warn_if_not_found", "write_temp_file", "xml_findall_deep", "xml_to_dict"
]

from download_pipelines.connection_utils import FTPConnection, SSHConnection
from download_pipelines.db_utils import config_from_dynamo, mysql_query
from download_pipelines.download_utils import (capitalize, contents,
                                                  date_from_str, download, get,
                                                  join_if_different_ids,
                                                  parse_csv, parse_json,
                                                  parse_xml, split,
                                                  str_from_date, strip, ungzip,
                                                  untar, unzip,
                                                  warn_if_not_found)
from download_pipelines.helper_utils import progress_bar, write_temp_file
from download_pipelines.logging_utils import set_logger
from download_pipelines.pipe_utils import (Pipe, concat, join, maybe,
                                              p_filter, p_map, p_print,
                                              p_reduce, set_inter, set_union,
                                              to_set)
from download_pipelines.xml_utils import xml_findall_deep, xml_to_dict
