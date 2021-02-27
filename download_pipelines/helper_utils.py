import functools
import os
import re
import sys
import tempfile
import time
from datetime import datetime
from multiprocessing.context import Process
from multiprocessing.queues import Queue
from typing import Optional
from urllib.request import urlopen

from download_pipelines.logging_utils import set_logger

logger = set_logger(__name__)


class Str(str):
    """
    A wrapper class for `str` to provide additional functionality, such as using regular expressions with `split` and
    `strip`. It includes a `date_format` method to convert strings to `datetime`s.
    """
    def split(self,
              sep: Optional[str] = "\W+",
              maxsplit: Optional[int] = None):
        if isinstance(maxsplit, int):
            return [Str(elem) for elem in re.split(sep, self, maxsplit)]
        return [Str(elem) for elem in re.split(sep, self)]

    def lstrip(self, regex: Optional[str] = r"\W+"):
        split = self.split(f"({regex})")
        for el in split.copy():
            if not el or re.match(regex, el):
                split.pop(0)
            else:
                break
        return Str("".join(split))

    def rstrip(self, regex: Optional[str] = r"\W+"):
        split = self.split(f"({regex})")
        for el in reversed(split.copy()):
            if not el or re.match(regex, el):
                split.pop()
            else:
                break
        return Str("".join(split))

    def strip(self, regex: Optional[str] = r"\W+"):
        return self.lstrip(regex).rstrip(regex)

    def format_date(self, _format: Optional[str] = "%Y-%m-%d") -> datetime:
        length = len(list(datetime.strftime(datetime.now(), _format)))
        return datetime.strptime(self.strip()[:length], _format)

    def capitalize(self):
        return Str("".join(map(str.capitalize, self.split(r"(\W+)"))))


def write_temp_file(name, contents):
    temp = tempfile.NamedTemporaryFile(mode="wb+", delete=False)
    temp_name = os.path.join("/tmp/", name.split("/")[-1])
    os.rename(os.path.join("/tmp/", temp.name), temp_name)
    if contents:
        if isinstance(contents, str):
            contents = contents.encode()
        temp.write(contents)
    logger.debug("Saved temp file %s" % temp.name)
    return temp_name


def url_download(url: str):
    logger.info("Downloading from %s" % url)
    with urlopen(url) as f:
        temp_name = write_temp_file(url, f.read())
    logger.info("File downloaded to %s" % temp_name)
    return temp_name


def progress_bar(iterator, length):
    """
    A simple progress bar that can be used as a decorator

    @param iterator: the decorated function will be called with the elements of iterator
    @param length: the number of times the decorated function will be called
    """
    def decorator(function):
        @functools.wraps(function)
        def func(*args, **kwargs):
            deltas, before, after = [], 0, 0
            sys.stdout.write(" " * 147)
            for item in iterator:
                if isinstance(item, tuple):
                    args = list(item) + list(args)
                    item = item[0]
                else:
                    args = [item] + args
                delta = after - before
                eta = 0
                if delta > 0:
                    deltas.append(delta)
                    eta = (length - item) * (sum(deltas) / len(deltas))
                percentage = 100 * item / length
                p = Process(target=draw_progress_bar, args=(eta, percentage, f"{item + 1} of {length}..."))
                before = time.time()
                p.start()
                function(*args, **kwargs)
                p.terminate()
                after = time.time()
            p = Process(target=draw_progress_bar, args=(0, 100, "Ready!"))
            p.start()
            time.sleep(0.1)
            p.terminate()
            print()

        return func

    return decorator


def draw_progress_bar(eta, percentage, message=None):
    percentage, eta = round(percentage), round(eta)
    while True:
        bar = [
            f"【{'🍿' * int(48 * percentage / 100)}",
            f"{'  ' * int(48 * (1 - percentage / 100))}】"
        ]
        message = (message[-20:]
                   if len(message) > 20 else message) if message else " "
        string = f"{message: >20} {percentage:>10.0f}%  ETA: {eta:>5.0f}s    {''.join(bar)}"
        sys.stdout.write("\b" * 147)
        sys.stdout.write(string)
        sys.stdout.flush()
        one_percent_time = (eta / (100 - percentage)) if eta > 0 else 0.1
        percent_increase_per_second = 1 / one_percent_time
        time.sleep(1)
        if eta > 0:
            eta -= 1
        if eta > 0 and percentage > 0:
            percentage += percent_increase_per_second


if __name__ == '__main__':

    @progress_bar(enumerate(range(10)), 10)
    def do_nothing(*args):
        time.sleep(3)

    do_nothing()
