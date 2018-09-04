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

from .. import algobase


class Algo_base_SdssShape(algobase.Algo):
    positions = [
        {
            'x'  : 'base_SdssShape_x',
            'y'  : 'base_SdssShape_y',
            'ra' : 'base_SdssShape_ra',
            'dec': 'base_SdssShape_dec',
        },
    ]

    fluxes = [
        {
            "flux": 'base_SdssShape_flux',
        },
    ]
    fluxerrs = [
        {
            "flux": 'base_SdssShape_flux',
            "fluxerr": 'base_SdssShape_fluxSigma',
        },
    ]

    shapes = [
        {
            "xx": 'base_SdssShape{}_xx'.format(infix),
            "yy": 'base_SdssShape{}_yy'.format(infix),
            "xy": 'base_SdssShape{}_xy'.format(infix),
            "11": 'base_SdssShape{}_shape11'.format(infix),
            "22": 'base_SdssShape{}_shape22'.format(infix),
            "12": 'base_SdssShape{}_shape12'.format(infix),
            "ra": "base_SdssShape_ra",
            "dec": "base_SdssShape_dec",
        }
        for infix in ["", "_psf"]
    ] + [
        {
            "xx": 'base_SdssShape_flux_xx_Cov',
            "yy": 'base_SdssShape_flux_yy_Cov',
            "xy": 'base_SdssShape_flux_xy_Cov',
            "11": 'base_SdssShape_flux_shape11_Cov',
            "22": 'base_SdssShape_flux_shape22_Cov',
            "12": 'base_SdssShape_flux_shape12_Cov',
            "ra": "base_SdssShape_ra",
            "dec": "base_SdssShape_dec",
        },
    ]

    shapesigmas = [
        {
            "xxsigma": 'base_SdssShape_xxSigma',
            "yysigma": 'base_SdssShape_yySigma',
            "xysigma": 'base_SdssShape_xySigma',
            "11sigma": 'base_SdssShape_shape11Sigma',
            "22sigma": 'base_SdssShape_shape22Sigma',
            "12sigma": 'base_SdssShape_shape12Sigma',
            "ra": "base_SdssShape_ra",
            "dec": "base_SdssShape_dec",
        },
    ]

    renamerules = [
        (r'base_', ''),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("base_SdssShape_")
