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

import numpy

from . import misc
from . import fits

@misc.cached
def read_wcs(imagePath):
    """
    Get Wcs object from a file.
    @param imagePath
        Path to the image file
    @return
        Wcs object.
    """
    header = fits.fits_open(imagePath)[1].header
    return Wcs(header)


class Wcs(object):
    """
    World coordinate system.
    """
    def __init__(self, fitsheader):
        """
        @param fitsheader
            A dictionary object, probably a FITS header,
            with sufficient (key, value) pairs to construct the WCS.
        """
        toRadian = numpy.pi / 180.0
        self.crpix1a = fitsheader["CRPIX1A"]
        self.crpix2a = fitsheader["CRPIX2A"]
        self.crval1a = fitsheader["CRVAL1A"]
        self.crval2a = fitsheader["CRVAL2A"]
        self.crpix1  = fitsheader["CRPIX1"]
        self.crpix2  = fitsheader["CRPIX2"]
        self.crval1  = toRadian * fitsheader["CRVAL1"]
        self.crval2  = toRadian * fitsheader["CRVAL2"]
        self.cd      = toRadian * numpy.array([
            [fitsheader.get("CD1_1", 0.0), fitsheader.get("CD1_2", 0.0)],
            [fitsheader.get("CD2_1", 0.0), fitsheader.get("CD2_2", 0.0)],
        ], dtype=float)

    def pixeltosky(self, x, y, outIsDegree = True):
        """
        Convert (x,y) to the sky coordinates.
        (x,y) are in "A" coordinates defined by CRPIX1/2A and CRVAL1/2A
        """
        x = numpy.asarray(x)
        y = numpy.asarray(y)

        # pixel coord => intermediate world coord (IWC)
        p = x + self.crpix1a - self.crval1a - self.crpix1
        q = y + self.crpix2a - self.crval2a - self.crpix2

        # (note that (x,y) changes its meaning; it hereafter means IWC)
        x = self.cd[0,0] * p + self.cd[0,1] * q
        y = self.cd[1,0] * p + self.cd[1,1] * q

        # IWC => native spherical coord (NSC)
        R = numpy.hypot(x, y)
        # cond = (R <= 0.0)
        # theta = numpy.where(cond, numpy.pi/2.0, numpy.arctan(1.0/R))
        # phi   = numpy.where(cond, 0.0         , numpy.arctan2(x, -y))

        # NSC => celestial spherical coord (CSC)
        # lonpole = numpy.where(arr["crval2"] == 90.0 * (numpy.pi/180.0), 0.0, numpy.pi)
        # phi -= lonpole
        if self.crval2 != 90.0 * (numpy.pi/180.0):
            x *= -1.0
            y *= -1.0

        # sin_phi   = numpy.sin(phi)
        # cos_phi   = numpy.cos(phi)
        I_R = 1.0 / R
        cond = (R <= 0.0)
        sin_phi = numpy.where(cond, 0.0, x  * I_R)
        cos_phi = numpy.where(cond, 1.0, -y * I_R)

        # sin_theta = numpy.sin(theta)
        # cos_theta = numpy.cos(theta)
        I_R1 = 1.0 / numpy.hypot(R, 1.0)
        sin_theta = I_R1
        cos_theta = R * I_R1

        sin_crv2  = numpy.sin(self.crval2)
        cos_crv2  = numpy.cos(self.crval2)

        # (note that (x,y) changes its meaning again here)
        x = sin_theta*cos_crv2 - cos_theta*sin_crv2*cos_phi
        y = -cos_theta * sin_phi

        ra = numpy.where((x == 0) & (y == 0),
            0.0,
            self.crval1 + numpy.arctan2(y, x)
        )

        if outIsDegree:
            ra *= 180.0 / numpy.pi
            numpy.remainder(ra, 360.0, out=ra)

        sin_dec = sin_theta*sin_crv2 + cos_theta*cos_crv2*cos_phi
        numpy.clip(sin_dec, -1.0, 1.0, out=sin_dec)

        dec = numpy.arcsin(sin_dec)
        if outIsDegree:
            dec *= 180.0 / numpy.pi

        return ra, dec

    def pixeltosky_get_jacobian(self, ra, dec):
        """
        Get Jacobian of pixeltosky() at (ra, dec)
        @param ra
            ra of a galaxy (or galaxies)
        @param dec
            dec of a galaxy (or galaxies)
        @return
            WcsJacobian([
              [ J11, J12 ],
              [ J21, J22 ],
            ])
        """
        ra  = numpy.asarray(ra , dtype=float) * (numpy.pi / 180.0)
        dec = numpy.asarray(dec, dtype=float) * (numpy.pi / 180.0)
        e1 , e2 , t  = Wcs.pixeltosky_get_tangential_basis(ra, dec)
        e10, e20, t0 = Wcs.pixeltosky_get_tangential_basis(self.crval1, self.crval2)

        t_t0 = numpy.sum(t * t0, axis = -1)

        J11 = t_t0 * numpy.sum(e1 * e10, axis = -1)
        J12 = t_t0 * numpy.sum(e1 * e20, axis = -1)
        J21 = t_t0 * numpy.sum(e2 * e10, axis = -1)
        J22 = t_t0 * numpy.sum(e2 * e20, axis = -1)

        # The above Jacobian is dSky / dIWC
        # but we need is dSky / dPix = (dSky/dIWC) * (dIWC/dPix)

        Jp11 = J11*self.cd[0,0] + J12*self.cd[1,0]
        Jp12 = J11*self.cd[0,1] + J12*self.cd[1,1]
        Jp21 = J21*self.cd[0,0] + J22*self.cd[1,0]
        Jp22 = J21*self.cd[0,1] + J22*self.cd[1,1]

        return WcsJacobian([
            [ Jp11, Jp12 ],
            [ Jp21, Jp22 ],
        ])

    @staticmethod
    def pixeltosky_get_tangential_basis(ra, dec):
        """
        @return
            [e1, e2, t]

        * t is the unit vector corresponding to (ra, dec)
        * e1 is the unit vector whose direction agrees with dt/dra
        * e2 is the unit vector whose direction agrees with dt/ddec
        """

        ra   = numpy.asarray(ra , dtype=float)
        dec  = numpy.asarray(dec, dtype=float)

        cos_ra   = numpy.cos(ra  )
        sin_ra   = numpy.sin(ra  )
        cos_dec  = numpy.cos(dec )
        sin_dec  = numpy.sin(dec )

        destShape = ra.shape + (3,)

        t = numpy.empty(shape=destShape, dtype=float)
        t[...,0] = cos_ra * cos_dec
        t[...,1] = sin_ra * cos_dec
        t[...,2] = sin_dec

        e1 = numpy.empty(shape=destShape, dtype=float)
        e1[...,0] = -sin_ra
        e1[...,1] = cos_ra
        e1[...,2] = 0.0

        e2 = numpy.empty(shape=destShape, dtype=float)
        e2[...,0] = -cos_ra * sin_dec
        e2[...,1] = -sin_ra * sin_dec
        e2[...,2] = cos_dec

        return [ e1, e2, t ]


class WcsJacobian(object):
    def __init__(self, jacobian):
        """
        @param jacobian: jacobian[i][j] = d(sky_i) / d(x_j)
            Here "sky" is the coord on the tangential plane.
            For each i,j; jacobian[i][j] is a numpy array.
        """
        self.J = jacobian

    def pixel_scale(self, outIsArcsec=True):
        """
        Get pixel size
        """
        pixel_scale = numpy.sqrt(numpy.abs(self.J[0][0]*self.J[1][1] - self.J[0][1]*self.J[1][0]))
        if outIsArcsec:
            pixel_scale *= 180.0*3600.0 / numpy.pi

        return pixel_scale

    def pixeltosky_err(self, err_xx, err_xy, err_yy, outIsArcsec=True):
        """
        Convert positional error (covariance)
        """
        # \vec{x}: pixel coord
        # \vec{s}: sky coord
        #
        # err = average of x x^T
        # ret = average of s s^T = average of (Jx) (x^T J^T) = J err J^T

        J = self.J

        err00 = err_xx
        err01 = err_xy
        err10 = err_xy
        err11 = err_yy

        je00 = J[0][0] * err00 + J[0][1] * err10
        je01 = J[0][0] * err01 + J[0][1] * err11
        je10 = J[1][0] * err00 + J[1][1] * err10
        je11 = J[1][0] * err01 + J[1][1] * err11

        err00 = je00 * J[0][0] + je01 * J[0][1]
        err01 = je00 * J[1][0] + je01 * J[1][1]
        err11 = je10 * J[1][0] + je11 * J[1][1]

        if outIsArcsec:
            toArcsec = (180.0*3600.0 / numpy.pi)**2
            err00 *= toArcsec
            err01 *= toArcsec
            err11 *= toArcsec

        return err00, err01, err11

    def pixeltosky_err_diag(self, err_xx, err_yy, outIsArcsec=True):
        """
        Convert positional error (covariance).
        Only its diagonal parts are considered.
        """
        # \vec{x}: pixel coord
        # \vec{s}: sky coord
        #
        # err = average of x x^T
        # ret = average of s s^T = average of (Jx) (x^T J^T) = J err J^T

        J = self.J

        err00 = err_xx
        #err01 = err_xy
        #err10 = err_xy
        err11 = err_yy

        je00 = J[0][0] * err00
        je01 =                   J[0][1] * err11
        je10 = J[1][0] * err00
        je11 =                   J[1][1] * err11

        err00 = je00 * J[0][0] + je01 * J[0][1]
        #err01 = je00 * J[1][0] + je01 * J[1][1]
        err11 = je10 * J[1][0] + je11 * J[1][1]

        if outIsArcsec:
            toArcsec = (180.0*3600.0 / numpy.pi)**2
            err00 *= toArcsec
            #err01 *= toArcsec
            err11 *= toArcsec

        return err00, err11

    def pixeltosky_shape(self, shape_xx, shape_yy, shape_xy, outIsArcsec=True):
        """
        Convert quadrupole moments.
        """
        # I'_{ij} = \int I(p) x_i x_j d^2x / \int I(p) d^2x
        #  = \int I(p) x x^T d^2x / \int I(p) d^2x
        #  = \int I(p) J p p^T J^T det(J) d^2p / \int I(p) det(J) d^2p
        #  = J I J^T

        J = self.J

        i00 = shape_xx
        i01 = shape_xy
        i10 = shape_xy
        i11 = shape_yy

        # JI = J * I
        ji00 = J[0][0] * i00  + J[0][1] * i10
        ji01 = J[0][0] * i01  + J[0][1] * i11
        ji10 = J[1][0] * i00  + J[1][1] * i10
        ji11 = J[1][0] * i01  + J[1][1] * i11

        # JIJ = JI * J^T
        jij00 = ji00 * J[0][0]  + ji01 * J[0][1]
        jij01 = ji00 * J[1][0]  + ji01 * J[1][1]
        jij11 = ji10 * J[1][0]  + ji11 * J[1][1]

        if outIsArcsec:
            toArcsec = (180.0*3600.0 / numpy.pi)**2
            jij00 *= toArcsec
            jij01 *= toArcsec
            jij11 *= toArcsec

        return jij00, jij11, jij01

    def pixeltosky_shape_err(self, err_xx_xx, err_xx_yy, err_yy_yy, err_xx_xy, err_yy_xy, err_xy_xy, outIsArcsec=True):
        """
        Convert covariance of quadrupole moments.
        """
        J = self.J

        # C[i][j][k][l] = Cov(I[i][j], I[k][l])
        c0000 = err_xx_xx
        c0001 = err_xx_xy
        c0010 = c0001
        c0011 = err_xx_yy
        c0100 = c0001
        c0101 = err_xy_xy
        c0110 = c0101
        c0111 = err_yy_xy
        c1000 = c0001
        c1001 = c0101
        c1010 = c0101
        c1011 = c0111
        c1100 = c0011
        c1101 = c0111
        c1110 = c0111
        c1111 = err_yy_yy

        # C'[i][j][k][l] = J[i][m] J[j][n] J[k][o] J[l][p] C[m][n][o][p]
        def Cprime(i,j,k,m):
            return (
                J[i][0] * J[j][0] * J[k][0] * J[m][0] * c0000
            +   J[i][0] * J[j][0] * J[k][0] * J[m][1] * c0001
            +   J[i][0] * J[j][0] * J[k][1] * J[m][0] * c0010
            +   J[i][0] * J[j][0] * J[k][1] * J[m][1] * c0011
            +   J[i][0] * J[j][1] * J[k][0] * J[m][0] * c0100
            +   J[i][0] * J[j][1] * J[k][0] * J[m][1] * c0101
            +   J[i][0] * J[j][1] * J[k][1] * J[m][0] * c0110
            +   J[i][0] * J[j][1] * J[k][1] * J[m][1] * c0111
            +   J[i][1] * J[j][0] * J[k][0] * J[m][0] * c1000
            +   J[i][1] * J[j][0] * J[k][0] * J[m][1] * c1001
            +   J[i][1] * J[j][0] * J[k][1] * J[m][0] * c1010
            +   J[i][1] * J[j][0] * J[k][1] * J[m][1] * c1011
            +   J[i][1] * J[j][1] * J[k][0] * J[m][0] * c1100
            +   J[i][1] * J[j][1] * J[k][0] * J[m][1] * c1101
            +   J[i][1] * J[j][1] * J[k][1] * J[m][0] * c1110
            +   J[i][1] * J[j][1] * J[k][1] * J[m][1] * c1111
            )

        err_xx_xx = Cprime(0,0,0,0)
        err_xx_yy = Cprime(0,0,1,1)
        err_yy_yy = Cprime(1,1,1,1)
        err_xx_xy = Cprime(0,0,0,1)
        err_yy_xy = Cprime(0,1,1,1)
        err_xy_xy = Cprime(0,1,0,1)

        if outIsArcsec:
            toArcsec = (180.0*3600.0 / numpy.pi)**4
            err_xx_xx *= toArcsec
            err_xx_yy *= toArcsec
            err_yy_yy *= toArcsec
            err_xx_xy *= toArcsec
            err_yy_xy *= toArcsec
            err_xy_xy *= toArcsec

        return err_xx_xx, err_xx_yy, err_yy_yy, err_xx_xy, err_yy_xy, err_xy_xy

    def pixeltosky_shape_err_diag(self, err_xx_xx, err_yy_yy, err_xy_xy, outIsArcsec=True):
        """
        Convert covariance of quadrupole moments. Only its diagonal parts are considered.
        """
        J = self.J

        # C[i][j][k][l] = Cov(I[i][j], I[k][l])
        c0000 = err_xx_xx
        c0101 = err_xy_xy
        c0110 = c0101
        c1001 = c0101
        c1010 = c0101
        c1111 = err_yy_yy

        # C'[i][j][k][l] = J[i][m] J[j][n] J[k][o] J[l][p] C[m][n][o][p]
        def Cprime(i,j,k,m):
            return (
                J[i][0] * J[j][0] * J[k][0] * J[m][0] * c0000
            # +   J[i][0] * J[j][0] * J[k][0] * J[m][1] * c0001
            # +   J[i][0] * J[j][0] * J[k][1] * J[m][0] * c0010
            # +   J[i][0] * J[j][0] * J[k][1] * J[m][1] * c0011
            # +   J[i][0] * J[j][1] * J[k][0] * J[m][0] * c0100
            +   J[i][0] * J[j][1] * J[k][0] * J[m][1] * c0101
            +   J[i][0] * J[j][1] * J[k][1] * J[m][0] * c0110
            # +   J[i][0] * J[j][1] * J[k][1] * J[m][1] * c0111
            # +   J[i][1] * J[j][0] * J[k][0] * J[m][0] * c1000
            +   J[i][1] * J[j][0] * J[k][0] * J[m][1] * c1001
            +   J[i][1] * J[j][0] * J[k][1] * J[m][0] * c1010
            # +   J[i][1] * J[j][0] * J[k][1] * J[m][1] * c1011
            # +   J[i][1] * J[j][1] * J[k][0] * J[m][0] * c1100
            # +   J[i][1] * J[j][1] * J[k][0] * J[m][1] * c1101
            # +   J[i][1] * J[j][1] * J[k][1] * J[m][0] * c1110
            +   J[i][1] * J[j][1] * J[k][1] * J[m][1] * c1111
            )

        err_xx_xx = Cprime(0,0,0,0)
        err_yy_yy = Cprime(1,1,1,1)
        err_xy_xy = Cprime(0,1,0,1)

        if outIsArcsec:
            toArcsec = (180.0*3600.0 / numpy.pi)**4
            err_xx_xx *= toArcsec
            err_yy_yy *= toArcsec
            err_xy_xy *= toArcsec

        return err_xx_xx, err_yy_yy, err_xy_xy

    def pixeltosky_ecc(self, e1, e2):
        """
        Convert eccentricity of the third kind:
            e = (a^2 - b^2) / (a^2 + b^2),
            e1 = e cos(2theta), e2 = e sin(2theta)
        """
        shape_xx = 1.0 + e1
        shape_yy = 1.0 - e1
        shape_xy = e2

        shape_xx, shape_yy, shape_xy = self.pixeltosky_shape(shape_xx, shape_yy, shape_xy, outIsArcsec=False)

        denom = 1.0 / (shape_xx + shape_yy)

        shape_xx -= shape_yy
        shape_xy *= 2.0

        shape_xx *= denom
        shape_xy *= denom

        return shape_xx, shape_xy # = (e1, e2)
