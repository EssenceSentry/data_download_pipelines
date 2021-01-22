import functools
import itertools
import warnings
from collections.abc import Iterable, Mapping, Set

class Pipe:
    """
    Implements a Unix like pipe, where the value before the pipe (`|`) is passed as a parameter to the next function.

    >>> range(10) | Pipe(lambda x: x + [2]) | Pipe(lambda x: len(x) > 5)
    True

    This class decorates the callables with `maybe`, so any exception that might occur during execution will return 
    `None` and raise a warning instead of raising an exception. This behavior is useful in pipelines where a simple 
    error in, for example, a map, can break the process. See `p_map` for examples of usage.

    Other niceties added are:

    - Calling `flatten` with the returned result to avoid deep nesting of lists,
    which is particularly useful when many calls to map are made:

    >>> 3 | Pipe(lambda x: [[[x]]])
    [3]

    - Calling `safe_iter` with the input parameter to avoid problems with iterables that can be consumed or that lack
    a `list` interface:

    >>> range(10) | Pipe(lambda x: x + [2])
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 2]

    While:

    >>> range(10) + [2]
    Traceback (most recent call last):
    ...
    TypeError: unsupported operand type(s) for +: 'range' and 'list'


    Some examples of pipelines:

    >>> ([1, 2, 3] | join(range(10)) | p_map(lambda x: x + 1) | p_filter(lambda x: x % 2 == 0)
    ... | join(range(10), lambda x, it: x not in it))
    [2, 4, 2, 4, 6, 8, 10, 0, 1, 3, 5, 7, 9]

    >>> ([1, 2, 3] | join(range(10)) | p_map(lambda x: x + 1) | p_filter(lambda x: x % 2 == 0)
    ... | join(range(10), lambda x, it: x / x == 1))
    [2, 4, 2, 4, 6, 8, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    >>> [1, 2, 3, 4, ""] | p_map(int)
    [1, 2, 3, 4]

    @param function: a callable that is called with the value passed by the `|` operator
    @return: a `Pipe` instance that can be used with the `|` operator
    """
    def __init__(self, function):
        self.function = maybe(function)
        functools.update_wrapper(self, function)

    def __or__(self, other):
        return Pipe(lambda it: it | self | other)

    def __ror__(self, other):
        return flatten(self.function(safe_iter(other)))

    def __call__(self, *args, **kwargs):
        return Pipe(lambda x: self.function(x, *args, **kwargs))


def safe_iter(it):
    """
    In Python many iterables do not provide the common and predictable interfaces we expect (such as `list`,
    `set` or `dict`), so, working with them can be a source of confusion if we are not aware of the type of iterables
    we are expecting from other functions. Some of these can even be consumed during iteration, which makes our
    programs hard to reason with and debug. This function sacrifices some performance for the sake of predictability.

    >>> d = safe_iter({"a": {"deep": "dictionary"}, "b": map(lambda x: x + 2, range(10))})
    >>> d
    {'a': {'deep': 'dictionary'}, 'b': [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]}
    >>> safe_iter(d.items())
    [['a', {'deep': 'dictionary'}], ['b', [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]]]
    >>> safe_iter(map(lambda x: x + 2, range(10)))
    [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    >>> safe_iter(range(10))
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> safe_iter(filter(lambda x: x % 2, range(20)))
    [1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    >>> safe_iter(set(range(10)))
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    >>> safe_iter((2, 3, (4, 5), range(10)))
    [2, 3, [4, 5], [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]]

    @param it: Any value, but only provides functionality for iterables.
    @return: A `list`, a `set` or a `dict`
    depending on the input type or the passed value type if it is not an iterable.
    """
    if isinstance(it, Iterable) and not isinstance(it, (bytes, str)):
        if isinstance(it, Mapping):
            return {k: safe_iter(v) for (k, v) in it.items()}
        if isinstance(it, Set):
            try:
                return {safe_iter(elem) for elem in it}
            except TypeError:
                pass
        return [safe_iter(elem) for elem in it]
    return it


def flatten(it):
    """
    Avoid unnecessary nesting of lists.

    >>> flatten([[["item"]]])
    ['item']
    >>> flatten("item")
    'item'
    >>> flatten({"one": "item"})
    {'one': 'item'}
    >>> flatten([[[1]], [[[[[2]]]]]])
    [[1], [2]]
    >>> flatten([[[1]], [[[[[2]]]]]]) | concat
    [1, 2]
    >>> flatten({"a": {"deep": {"nested": {"dict": [[[1]], [[[[[2]]]]]]}}}})
    {'a': {'deep': {'nested': {'dict': [[1], [2]]}}}}
    >>> flatten(range(10))
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    @param it: An iterable
    @return: The iterable flattened
    """
    it = safe_iter(it)
    if isinstance(it, dict):
        return {k: flatten(v) for k, v in it.items()}
    if isinstance(it, list) and len(it) > 0:
        if len(it) == 1 and isinstance(it[0], list):
            return flatten(it[0])
        return [flatten(elem) for elem in it]
    return it


def maybe(function):
    """
    Decorates a function that might raise an exception and returns None instead (raising a warning message).

    @param function: function that might fail
    @return: decorated function
    """
    @functools.wraps(function)
    def func(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as e:
            warnings.warn("Returning None instead of exception: %s" % str(e))

    return func


@Pipe
def filter_none(iterable):
    return filter(lambda x: x is not None, iterable)


@Pipe
def p_map(iterable, function):
    """
    A `Pipe` instance that implements map. If an instance of Mapping, string, bytes or a non-iterable object is
    passed, it encapsulates it in a list, because it is the best behavior for working with pipelines. This
    implementation also filters out None values because they are probably the result of a failed operation.

    >>> range(10) | p_map(lambda x: x + 1)
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    >>> range(10) | p_map(Pipe(lambda x: x + 1))
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    """
    if isinstance(function, Pipe):
        return map(lambda x: x | function, iterable) | filter_none
    return map(maybe(function), iterable) | filter_none


@Pipe
def p_filter(iterable, function):
    """
    A `Pipe` instance that implements filter. See `p_map`.

    >>> range(10) | p_filter(lambda x: x % 2)
    [1, 3, 5, 7, 9]
    >>> range(10) | p_filter(Pipe(lambda x: x % 2))
    [1, 3, 5, 7, 9]
    """
    if isinstance(function, Pipe):
        return filter(lambda x: x | function, iterable) | filter_none
    return filter(maybe(function), iterable) | filter_none


@Pipe
def p_reduce(iterable, function):
    """
    A `Pipe` instance that implements reduce. See `p_map`.
    """
    return list(functools.reduce(function, iterable))


@Pipe
def join(iterable1, iterable2, function=lambda it2_elem, it1: True):
    """
    Returns a list joining `iterable1` and `iterable2`, previously filtering `iterable2` with `function`.

    >>> range(10) | join(range(5, 15), lambda x, it1: x not in it1)
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

    >>> ["hello, "] | join(["world!"])
    ['hello, ', 'world!']

    >>> ["hello, "] | join(["world!"]) | Pipe(lambda x: "".join(x))
    'hello, world!'

    """
    iterable1 = flatten(iterable1)
    iterable2 = iterable2 | p_filter(lambda x: function(x, iterable1))
    return iterable1 + iterable2


@Pipe
def concat(iterables, function=None):
    """
    Returns a list joining `iterables` filtering successive iterables with `function`.

    >>> [range(10), range(5, 15), range(10, 20)] | concat(lambda x, it: x not in it)
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    >>> [range(10), range(5, 15), range(10, 20)] | concat
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    """
    if not function:
        if iterables and isinstance(iterables[0], list):
            return list(itertools.chain.from_iterable(iterables))
        else:
            return iterables
    return iterables | p_reduce(lambda x, y: x | join(y, function))


@Pipe
def to_set(iterable):
    return iterable | Pipe(lambda it: set(it))


@Pipe
def set_inter(iterable):
    """
    >>> [range(10), range(5, 15)] | set_inter()
    {5, 6, 7, 8, 9}
    """
    return iterable | p_map(lambda it: set(it)) | p_reduce(
        lambda x, y: x.intersection(y)) | to_set()


@Pipe
def set_union(iterable):
    """
    >>> [range(10), range(5, 15)] | set_union()
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
    """
    return iterable | p_map(lambda it: set(it)) | p_reduce(
        lambda x, y: x.union(y)) | to_set()


@Pipe
def p_print(iterable):
    """
    Just for debugging purposes. It prints the input and returns it.

    >>> range(10) | p_print | p_map(lambda x: x + 1) | p_print
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    """
    print(str(iterable)[:min(9999, len(str(iterable)))])
    return iterable


if __name__ == '__main__':
    import doctest
    doctest.testmod()
