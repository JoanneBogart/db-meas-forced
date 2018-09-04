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

import itertools

from .. import algobase
from .. import common

def _rename_aperture_size(m):
    prefix, radius, radius_frac, suffix = m.groups()
    radius = float(radius + "." + radius_frac)

    diameter = common.defaultPixelScale * 2 * radius
    diameter = str(int(round(diameter*10)))

    return prefix + diameter + suffix


class Algo_ext_convolved_ConvolvedFlux(algobase.Algo):
    sizes = [
        {
            "size": 'ext_convolved_ConvolvedFlux_seeing',
            "ra": 'default_ra',
            "dec": 'default_dec',
        },
    ]

    fluxes = [
        {
            "flux": 'ext_convolved_ConvolvedFlux_{}_{}_flux'.format(seeing, size),
        }
        for seeing, size in itertools.product(
            ["0", "1", "2", "3"],
            ["3_3", "4_5", "6_0", "kron"],
        )
    ]

    fluxerrs = [
        {
            "flux": 'ext_convolved_ConvolvedFlux_{}_{}_flux'.format(seeing, size),
            "fluxerr": 'ext_convolved_ConvolvedFlux_{}_{}_fluxSigma'.format(seeing, size),
        }
        for seeing, size in itertools.product(
            ["0", "1", "2", "3"],
            ["3_3", "4_5", "6_0", "kron"],
        )
    ]

    renamerules = [
        (r'ext_convolved_(ConvolvedFlux_[0-9]+_)([0-9]+)_([0-9]+)(_)', _rename_aperture_size),
        (r'ext_convolved_', ''),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("ext_convolved_ConvolvedFlux_")
