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
    "calib_",                  # not in for LSST run1.1; is in run1.2
    "deblend_",
    "ext_convolved_ConvolvedFlux_",
    "ext_photometryKron_KronFlux_", # not in for LSST run1.1; is in 1.2
    "ext_shapeHSM_HsmPsfMoments_",
    "ext_shapeHSM_HsmShapeRegauss_",
    "ext_shapeHSM_HsmSourceMoments_",
    "ext_shapeHSM_HsmSourceMomentsRound_", # not in for LSST run1.1; is in 1.2
    "ext_shapeHSM_HsmShapeBj_",                 # added for LSST
    "ext_shapeHSM_HsmShapeKsb_",                # added for LSST
    "ext_shapeHSM_HsmShapeLinear_",             # added for LSST
    "footprint",
    "modelfit_CModel_",
    "modelfit_DoubleShapeletPsfApprox_",
    #"subaru_FilterFraction_",                 # doesn't exist for LSST
]


forced_algos = PoppingOrderedDict(
    (_name, _import_algo(_name))
    for _name in (
        "base_CircularApertureFlux",
        "base_ClassificationExtendedness",  # only in ref for LSST run1.1
        "base_GaussianFlux",
        "base_InputCount",
        "base_LocalBackground",
        "base_PixelFlags",
        "base_PsfFlux",
        "base_SdssCentroid",
        "base_SdssShape",
        "base_Variance",
        "ext_convolved_ConvolvedFlux", # in ref only for 1.1; in forced for 1.2
        "ext_photometryKron_KronFlux", # not in LSST 1.1; is in 1.2
        "modelfit_CModel",
        "modelfit_DoubleShapeletPsfApprox",
        "undeblended_base_PsfFlux",                # Not in LSST Run1.1; in 1.2
        "undeblended_ext_photometryKron_KronFlux", # not in LSST 1.1; in 1.2
        "undeblended_base_CircularApertureFlux",   # not in LSST 1.1; in 1.2
    )
)

# There is an algorithm for undeblended_ext_convolve_ConvolvedFlux, so it's
# not clear we should be ignoring it.
forced_algos_ignored = [
    "coord",
    "parent",
    "deblend_nChild",
    "base_TransformedCentroid_",
    "base_TransformedShape_",
    "undeblended_ext_convolved_ConvolvedFlux_", # add for LSST 1.2
    "modelfit_GeneralShapeletPsfApprox_",       # added for LSST
]
