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

ref_algos = PoppingOrderedDict(
    (_name, _import_algo(_name))
    for _name in (
        "ref_coord",
        "detect",
        "merge",
    )
)

ref_algos_ignored = [
    "base_Blendedness_",
    "base_CircularApertureFlux_",
    "base_ClassificationExtendedness_",
    "base_FootprintArea_",
    "base_GaussianCentroid_",
    "base_GaussianFlux_",
    "base_InputCount_",
    "base_LocalBackground_",
    "base_NaiveCentroid_",
    "base_PixelFlags_",
    "base_PsfFlux_",
    "base_SdssCentroid_",
    "base_SdssShape_",
    "base_Variance_",
    "calib_",
    "deblend_",
    "ext_convolved_ConvolvedFlux_",
    "ext_photometryKron_KronFlux_",
    "ext_shapeHSM_HsmPsfMoments_",
    "ext_shapeHSM_HsmShapeRegauss_",
    "ext_shapeHSM_HsmSourceMoments_",
    "ext_shapeHSM_HsmSourceMomentsRound_",
    "footprint",
    "modelfit_CModel_",
    "modelfit_DoubleShapeletPsfApprox_",
    "subaru_FilterFraction_",
]


forced_algos = PoppingOrderedDict(
    (_name, _import_algo(_name))
    for _name in (
        "base_CircularApertureFlux",
        "base_ClassificationExtendedness",
        "base_GaussianFlux",
        "base_InputCount",
        "base_LocalBackground",
        "base_PixelFlags",
        "base_PsfFlux",
        "base_SdssCentroid",
        "base_SdssShape",
        "base_Variance",
        "ext_convolved_ConvolvedFlux",
        "ext_photometryKron_KronFlux",
        "modelfit_CModel",
        "modelfit_DoubleShapeletPsfApprox",
        "undeblended_base_PsfFlux",
        "undeblended_ext_photometryKron_KronFlux",
        "undeblended_base_CircularApertureFlux",
    )
)


forced_algos_ignored = [
    "coord",
    "parent",
    "deblend_nChild",
    "base_TransformedCentroid_",
    "base_TransformedShape_",
]