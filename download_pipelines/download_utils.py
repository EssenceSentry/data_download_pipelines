import copy
import logging
import warnings
from datetime import datetime

from download_pipelines import helper_utils, parse_utils, uncompress_utils
from download_pipelines.connection_utils import FTPConnection, SSHConnection
from download_pipelines.logging_utils import set_logger
from download_pipelines.pipe_utils import Pipe


logger = set_logger(__name__)
logging.getLogger("xml_utils").setLevel(logging.WARNING)


@Pipe
def download(filename, connection=None):
    if isinstance(connection, (FTPConnection, SSHConnection)):
        return connection.download(filename)
    if connection is None:
        return helper_utils.url_download(filename)
    raise ValueError(
        "Connection must be an instance of 'FTPConnection', 'SSHConnection' or 'None'. %s was given."
        % type(connection))


@Pipe
def contents(filename, connection):
    if isinstance(connection, (FTPConnection, SSHConnection)):
        return connection.contents(filename)
    raise ValueError(
        "Connection must be an instance of 'FTPConnection' or 'SSHConnection'. %s was given."
        % type(connection))


@Pipe
def warn_if_not_found(registers):
    if not registers:
        warnings.warn("No registers were found")
    return registers


@Pipe
def parse_csv(filename, delimiter="\t"):
    return parse_utils.parse_csv(filename, delimiter=delimiter)


@Pipe
def parse_xml(filename, tag):
    return parse_utils.parse_xml(filename, tag=tag)


@Pipe
def parse_json(filename):
    return parse_utils.parse_json(filename)


@Pipe
def split(string, sep="\W+"):
    """
    >>> "nov-dec-jan" | split
    ['nov', 'dec', 'jan']

    >>> ["nov-dec-jan", "feb-mar-apr-may"] | p_map(split) | concat
    ['nov', 'dec', 'jan', 'feb', 'mar', 'apr', 'may']
    """
    return helper_utils.Str(string).split(sep=sep)


@Pipe
def strip(string, regex="\W+"):
    """
    >>> ">>> Hello, World! <<<" | strip("[^\w!]")
    'Hello, World!'
    """
    return helper_utils.Str(string).strip(regex=regex)


@Pipe
def capitalize(string):
    """
    >>> "nov-dec-feb" | capitalize
    'Nov-Dec-Feb'
    """
    return helper_utils.Str(string).capitalize()


@Pipe
def date_from_str(string, date_format="%Y-%m-%d"):
    """
    >>> "jan-2021" | capitalize | date_from_str("%b-%Y")
    datetime.datetime(2021, 1, 1, 0, 0)
    
    >>> (range(9, 12) | p_map(lambda x: x + 1) | p_map(lambda x: f"2021-01-{x}") | join(["world!"]) |
    ... p_map(date_from_str("%Y-%m-%d")))
    [datetime.datetime(2021, 1, 10, 0, 0), datetime.datetime(2021, 1, 11, 0, 0), datetime.datetime(2021, 1, 12, 0, 0)]
    """
    return helper_utils.Str(string).format_date(date_format)


@Pipe
def str_from_date(string, date_format="%Y-%m-%d"):
    return datetime.strftime(string, date_format)


@Pipe
def unzip(filename, password=None):
    return uncompress_utils.unzip(filename, password=password)


@Pipe
def ungzip(filename):
    return uncompress_utils.ungzip(filename)


@Pipe
def untar(filename):
    return uncompress_utils.untar(filename)


@Pipe
def join_if_different_ids(iterables, id_column=None):
    """
    Our target is to allow repetition of IDs in each iterable, but not to add repeated registers from new iterables:

    >>> registers = ([[{"id": 1, "value": "one"}, {"id": 1, "value": "snd_one"}, {"id": 2, "value": "two"}],
    ...               [{"id": 1, "value": "alt_one"}, {"id": 3, "value": "three"}],
    ...               [{"id": 3, "value": "alt_three"}, {"id": 2, "value": "alt_two"}]])
    >>> registers | join_if_different_ids("id")
    [{'id': 1, 'value': 'one'}, {'id': 1, 'value': 'snd_one'}, {'id': 2, 'value': 'two'}, {'id': 3, 'value': 'three'}]
    """
    result, seen_ids = [], []
    for dict_list in iterables:
        filtered_dict_list = []
        dict_list_seen_ids = []
        for new_dict in dict_list:
            new_id = new_dict | get(id_column)
            if new_id not in seen_ids:
                dict_list_seen_ids.append(new_id)
                filtered_dict_list.append(new_dict)
        result += filtered_dict_list
        seen_ids += dict_list_seen_ids
    return result


@Pipe
def get(obj, key):
    """
    >>> [{2: "two", "one": 1}, {1: "uno"}] | get("0.2")
    'two'
    >>> registers = ([[{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
    ...               [{"id": 1, "value": "alt_one"}, {"id": 3, "value": "three"}]])
    >>> registers | p_map(p_map(get("id")))
    [[1, 2], [1, 3]]
    """
    result = copy.deepcopy(obj)
    try:
        keys = key.split(".")
        for k in keys:
            try:
                result = result[int(k)]
            except ValueError:
                result = result[k]
    except KeyError:
        result = None
    return result


if __name__ == "__main__":
    import doctest
    doctest.testmod()
