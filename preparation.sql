/*Copyright (C) 2016-2018  Sogo Mineo

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

BEGIN;

CREATE EXTENSION objcatalog CASCADE;

/* Make sure earth() has already been defined
    by "earthdistance" extension
*/
SELECT 'Original earth() is', earth();

/* Replace earth() so the unit of distance will be arcsecond.
*/
CREATE OR REPLACE FUNCTION
  earth
  ( OUT radius  Float8
  )
LANGUAGE SQL
IMMUTABLE
PARALLEL SAFE
AS $$
  -- 180*3600 / pi
  SELECT '206264.80624709636'::Float8;
$$;

COMMIT;
