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
Generate 'skymap_wcs' from 'skyMap.pickle'.
It can be used instead of 'calexp-*.fits' to get WCS.
"""

import lsst.skymap
import lsst.afw.image as afwImage

import pickle
import os
import sys

skyMapPath, = sys.argv[1:]
skyMap = pickle.load(open(skyMapPath, "rb"))

outDir = "skymap_wcs"

outDir = os.path.join(os.path.dirname(sys.argv[0]), outDir)

try:
    os.mkdir(outDir)
except OSError:
    pass

for tractNo in range(len(skyMap)):
    exp = afwImage.ExposureF(0,0)
    exp.setWcs(skyMap[tractNo].getWcs())

    outPath = os.path.join(outDir, "skymap_wcs-{tractNo}.fits".format(**locals()))
    print(outPath)

    exp.writeFits(outPath)
