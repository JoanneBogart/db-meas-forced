#!/usr/bin/env bash

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


# This program will update the copyright year
# at the beginning of each file in the working directory.
# For example, from
#   Copyright (C) 2016-2017  Jane Doe
# to
#   Copyright (C) 2016-2018  Jane Doe
# if it is 2018 when this script is executed.

author="Sogo Mineo"
year="$(date +"%Y")"

main()
(
  find . \( -name ".git" -prune -false \) -o -type f | while read filename
  do
    if  [[ "$(file "${filename}")" = *text* ]] &&
        grep -q "Copyright.*${author}" "${filename}"
    then
      python -c "${py_bumpup}" "${author}" "${year}" "${filename}"
    fi
  done
)

py_bumpup=$(cat << 'EOF'
import os
import re
import sys

author, year, filename = sys.argv[1:]
year = int(year)

def bumpup(match):
  prefix = match.group(1) or ""
  start  = int(match.group(2) or 0)
  end    = year

  if start == end:
    return prefix + str(start)
  else:
    return prefix + str(start) + "-" + str(end)

def main():
  with open(filename, "r") as f:
    input = f.read()
    output = re.sub(r"(Copyright *\(C\) *)([0-9]+)(-[0-9]+)?(?= *" + author + ")", bumpup, input)
  if input != output:
    mode = os.stat(filename).st_mode
    os.rename(filename, filename+"~")
    with open(filename, "w") as f:
      f.write(output)
      os.fchmod(f.fileno(), mode)

main()
EOF
)

main "$@"
