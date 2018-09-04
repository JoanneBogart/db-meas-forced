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

import itertools


class Algo_modelfit_CModel(algobase.Algo):
    fluxes = [
        {
            "flux": 'modelfit_CModel{}_flux{}'.format(infix, suffix)
        }
        for infix, suffix in itertools.product(
            ["_initial", "_exp", "_dev", ""],
            ["", "_inner"],
        )
    ]

    fluxerrs = [
        {
            "flux": 'modelfit_CModel{}_flux'.format(infix),
            "fluxerr": 'modelfit_CModel{}_fluxSigma'.format(infix)
        }
        for infix in ["_initial", "_exp", "_dev", ""]
    ]

    renamerules = [
        (r'modelfit_', ''),
    ]

    def __init__(self, sourceTable):
        self.sourceTable = sourceTable.cutout_subtable("modelfit_CModel_")
