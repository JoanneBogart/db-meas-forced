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

import contextlib
import io
import os
import sys


def open(format, *columns):
    desc_in, desc_out = os.pipe()
    pid = os.fork()
    if pid == 0:
        with contextlib.suppress(BaseException):
            os.close(desc_in)
        try:
            __open_child(desc_out, format, *columns)
            os._exit(0)
        except BaseException as e:
            with contextlib.suppress(BaseException):
                sys.excepthook(*sys.exc_info())
                sys.stdout.flush()
                sys.stderr.flush()

        os._exit(1)
    else:
        with contextlib.suppress(BaseException):
            os.close(desc_out)

        return PipeReadEnd(pid, desc_in)


def __open_child(desc_out, format, *columns):
    try:
        fout = io.open(desc_out, "wb")
    except:
        with contextlib.suppress(BaseException):
            os.close(desc_out)
        raise

    with fout:
        for tpl in zip(*columns):
            fout.write(format % tpl)


class PipeReadEnd(io.FileIO):
    def __init__(self, pid, desc):
        try:
            io.FileIO.__init__(self, desc, "rb")
        except:
            with contextlib.suppress(BaseException):
                os.close(desc)
            with contextlib.suppress(BaseException):
                os.waitpid(self.__pid, 0)
            raise

        self.__pid = pid

    def close(self):
        io.FileIO.close(self)
        if self.__pid != 0:
            pid = self.__pid
            self.__pid = 0
            _, status = os.waitpid(pid, 0)
            if not(os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0):
                raise RuntimeError("Thread that performed printf aborted.")
