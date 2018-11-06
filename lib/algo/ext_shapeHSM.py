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


class Algo_ext_shapeHSM(algobase.Algo):
    positions = [
        {
            'x'  : 'ext_shapeHSM_{}_x'.format(infix),
            'y'  : 'ext_shapeHSM_{}_y'.format(infix),
            'ra' : 'ext_shapeHSM_{}_ra'.format(infix),
            'dec': 'ext_shapeHSM_{}_dec'.format(infix),
        }
        for infix in ["HsmPsfMoments", "HsmSourceMoments", ]
        # DC2 data has no fields ext_shapeHSM_HsmSourceMomentsRound*
        #for infix in ["HsmPsfMoments", "HsmSourceMoments", "HsmSourceMomentsRound", ]
    ]

    shapes = [
        {
            'xx' : 'ext_shapeHSM_{}_xx'.format(infix),
            'yy' : 'ext_shapeHSM_{}_yy'.format(infix),
            'xy' : 'ext_shapeHSM_{}_xy'.format(infix),
            '11' : 'ext_shapeHSM_{}_shape11'.format(infix),
            '22' : 'ext_shapeHSM_{}_shape22'.format(infix),
            '12' : 'ext_shapeHSM_{}_shape12'.format(infix),
            'ra' : 'ext_shapeHSM_{}_ra'.format(infix),
            'dec': 'ext_shapeHSM_{}_dec'.format(infix),
        }
        for infix in ["HsmPsfMoments", "HsmSourceMoments", ]
        # DC2 data has no fields ext_shapeHSM_HsmSourceMomentsRound*
        #for infix in ["HsmPsfMoments", "HsmSourceMoments", "HsmSourceMomentsRound", ]
    ]

    #fluxes = [
    #    {
    #        #'flux': "ext_shapeHSM_HsmSourceMomentsRound_Flux",
    #        #'mag' : "ext_shapeHSM_HsmSourceMomentsRound_mag",
    #    }
    #]

    ellipticities = [
        {
            "e1": "ext_shapeHSM_HsmShapeRegauss_e1",
            "e2": "ext_shapeHSM_HsmShapeRegauss_e2",
            'ra' : 'ext_shapeHSM_HsmSourceMoments_ra',
            'dec': 'ext_shapeHSM_HsmSourceMoments_dec',
        },
    ]

    renamerules = [
        (r'ext_shapeHSM_', ''),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("ext_shapeHSM_")
