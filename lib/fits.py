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

try:
    import astropy.io.fits as pyfits
except ImportError:
    import pyfits

import numpy

import gzip
import io
import os
import re

def fits_open(path, headerOnly = False):
    """
    Open a FITS file ignoring the 3rd HDU and the latter ignored.
    The primary HDU must be empty.
    @param path
        Path to a FITS file to read.
        The file may be compressed, but "path" must ends with ".fits".
        The prefix ".gz" will be added automatically by this function.
    @param headerOnly
        Read header only.
    @return
        pyfits HDUList object.
    """
    header = b""
    dtype = numpy.dtype([("key", bytes, 8), ("value", bytes, 72)])

    if os.path.exists(path):
        fin = open(path, "rb")
    elif os.path.exists(path + ".gz"):
        fin = gzip.open(path + ".gz", "rb")
    else:
        raise RuntimeError("File inaccessible: " + path)

    # skip primary hdu (which is header-only)
    while True:
        chunk = fin.read(2880)
        arr = numpy.frombuffer(chunk, dtype=dtype)

        header += chunk
        if numpy.any(arr["key"] == b'END     '): break

    if headerOnly:
        while True:
            chunk = fin.read(2880)
            arr = numpy.copy(numpy.frombuffer(chunk, dtype=dtype))
            arr["value"][arr["key"] == b'NAXIS2  '] = b'=                    0 / length of data axis 2                          '

            header += memoryview(arr).tobytes()
            if numpy.any(arr["key"] == b'END     '): break
    else:
        bitpix = None
        width = None
        height = None
        while True:
            chunk = fin.read(2880)
            arr = numpy.frombuffer(chunk, dtype=dtype)
            header += chunk

            arrBitpix = arr["value"][arr["key"] == b'BITPIX  ']
            arrNaxis1 = arr["value"][arr["key"] == b'NAXIS1  ']
            arrNaxis2 = arr["value"][arr["key"] == b'NAXIS2  ']
            if len(arrBitpix) > 0:
                bitpix = int(re.match(br"^= *(-?[0-9]+)", arrBitpix[-1]).group(1))
            if len(arrNaxis1) > 0:
                width  = int(re.match(br"^= *([0-9]+)", arrNaxis1[-1]).group(1))
            if len(arrNaxis2) > 0:
                height = int(re.match(br"^= *([0-9]+)", arrNaxis2[-1]).group(1))

            if numpy.any(arr["key"] == b'END     '): break

        header += fin.read(((abs(bitpix)*width*height + (8*2880-1))//(8*2880))*2880)

    fin.close()
    return pyfits.open(io.BytesIO(header), uint=True)
