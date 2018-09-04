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

import importlib

from .misc import PoppingOrderedDict

def _import_algo(name):
    """
    Performs "from .algo.{name} import Algo_{name}"
    @return class Algo_{name} imported from .algo.{name}
    """
    return getattr(
        importlib.import_module("..algo." + name, __name__),
        "Algo_" + name,
    )

meas_algos = PoppingOrderedDict(
    (_name, _import_algo(_name))
    for _name in (
        "meas_coord",
        "base_Blendedness",
        "base_CircularApertureFlux",
        "base_ClassificationExtendedness",
        "base_FootprintArea",
        "base_GaussianFlux",
        "base_InputCount",
        "base_LocalBackground",
        "base_NaiveCentroid",
        "base_PixelFlags",
        "base_PsfFlux",
        "base_SdssCentroid",
        "base_SdssShape",
        "base_Variance",
        "calib",
        "deblend",
        "detect",
        "ext_convolved_ConvolvedFlux",
        "ext_photometryKron_KronFlux",
        "ext_shapeHSM",
        "merge",
        "meas_modelfit_CModel",
        "modelfit_DoubleShapeletPsfApprox",
        "subaru_FilterFraction",
    )
)

meas_algos_ignored = [
    "footprint",
]
