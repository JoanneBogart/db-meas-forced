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


class Algo_modelfit_DoubleShapeletPsfApprox(algobase.Algo):
    positions = [
        {
            'x'  : 'modelfit_DoubleShapeletPsfApprox_{}_x'.format(i),
            'y'  : 'modelfit_DoubleShapeletPsfApprox_{}_y'.format(i),
            'ra' : 'modelfit_DoubleShapeletPsfApprox_{}_ra'.format(i),
            'dec': 'modelfit_DoubleShapeletPsfApprox_{}_dec'.format(i),
        }
        for i in range(2)
    ]

    shapes = [
        {
            'xx' : 'modelfit_DoubleShapeletPsfApprox_{}_xx'.format(i),
            'yy' : 'modelfit_DoubleShapeletPsfApprox_{}_yy'.format(i),
            'xy' : 'modelfit_DoubleShapeletPsfApprox_{}_xy'.format(i),
            '11' : 'modelfit_DoubleShapeletPsfApprox_{}_shape11'.format(i),
            '22' : 'modelfit_DoubleShapeletPsfApprox_{}_shape22'.format(i),
            '12' : 'modelfit_DoubleShapeletPsfApprox_{}_shape12'.format(i),
            'ra' : 'modelfit_DoubleShapeletPsfApprox_{}_ra'.format(i),
            'dec': 'modelfit_DoubleShapeletPsfApprox_{}_dec'.format(i),
        }
        for i in range(2)
    ]

    renamerules = [
        (r'modelfit_', ''),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("modelfit_DoubleShapeletPsfApprox_")
