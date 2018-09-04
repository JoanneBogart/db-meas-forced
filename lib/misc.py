# Copyright (C) 2016-2018  Sogo Mineo
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import collections
import warnings


class _undefined:
    """
    "Undefined" value for PoppingOrderedDict.
    """

class PoppingOrderedDict(collections.OrderedDict):
    """
    Subclass of collections.OrderedDict.
    PoppingOrderedDict has "pop_many()" member.
    """
    def pop_many(self, keys, default=_undefined):
        """
        Pop many items. Popped items will be returned as another
        PoppingOrderedDict.
        @param keys (iterable):
            List of keys.
        @param default:
            Default value used when keys are not found.
            If this argument is not given, and a key is not found,
            an exception will be raised.
        @return (PoppingOrderedDict)
        """
        if default is _undefined:
            return PoppingOrderedDict(
                (k, self.pop(k)) for k in keys
            )
        else:
            return PoppingOrderedDict(
                (k, self.pop(k, default)) for k in keys
            )


def warning(*msg, **flags):
    """
    Show warning.
    Usage: warning("str1", "str2", ..., "strN", sep=', ')
    @param sep (optional)
        Separator. The printed message is "sep.join([str1, str2, ...])"
    """
    sep = flags.get('sep', ' ')
    warnings.warn(sep.join(str(i) for i in msg), stacklevel=2)


def cached(func):
    """
    Function decorator.
    On function calls, return values are cached so that function bodies
    are not executed twice for the same arguments.
    """
    retVal = {}
    def wrapper(*args):
        if args in retVal:
            return retVal[args]
        else:
            ret = func(*args)
            retVal[args] = ret
            return ret

    return wrapper


def meas_time(id):
    """
    Measure time of execution.
    Usage:
        x, y = obj.do_something(a, b, c=d)
    ->  x, y = meas_time("id")(obj.do_something)(a, b, c=d)

    Or this function can be used as a function decorator:
    @meas_time("id")
    def do_something(): ...

    Time of execution with the same "id" will be accumulated.
    """

    def _meas_time(func):
        def wrapper(*a, **b):
            import time
            start = time.time()
            ret = func(*a, **b)
            dt = time.time() - start

            sum = dt + _timeDict.get(id, 0.0)
            _timeDict[id] = sum

            print("time {}: {:.3f} sec (total {:.3f} sec)".format(id, dt, sum))

            return ret

        return wrapper

    return _meas_time

_timeDict = {}
