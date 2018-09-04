#!/usr/bin/env python

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

"""
For each 'Covariance(*)' columns in a given fits table,
check whether it is diagnoal or not
"""

import numpy
from lib.fits import pyfits
import sys, re

def main():
    fitsPath, = sys.argv[1:]

    hdu = pyfits.open(fitsPath, uint=True)[1]

    for ic in range(1, 1+hdu.header["TFIELDS"]):
        cclass = hdu.header.get("TCCLS{}".format(ic))
        if cclass in g_checkFunctions:
            name = hdu.header.get("TTYPE{}".format(ic))
            g_checkFunctions[cclass](to_safe_ident(name), hdu.data[name])

def check_cov_Moments(name, data):
    isnan = [ numpy.all(numpy.isnan(data[...,j])) for j in range(6) ]

    if isnan[1] and isnan[3] and isnan[4]:
        if isnan[0] and isnan[2] and isnan[5]:
            print("'{}': 'empty',".format(name))
        else:
            print("'{}': 'diagnoal',".format(name))
    else:
        print("'{}': 'full',".format(name))

def check_cov_Point(name, data):
    isnan = [ numpy.all(numpy.isnan(data[...,j])) for j in range(3) ]

    if isnan[1]:
        if isnan[0] and isnan[2]:
            print("'{}': 'empty',".format(name))
        else:
            print("'{}': 'diagnoal',".format(name))
    else:
        print("'{}': 'full',".format(name))


def to_safe_ident(name):
    """
    Convert an identifier to a safe one: [a-z_][a-z0-9_]*
    """
    name = name.lower()
    return re.sub(r"[^A-Za-z_]", "_", name[0]) + re.sub(r"[^A-Za-z0-9_]", "_", name[1:])


g_checkFunctions = {
    'Covariance(Moments)': check_cov_Moments,
    'Covariance(Point)': check_cov_Point,
}

if __name__ == "__main__":
    main()
