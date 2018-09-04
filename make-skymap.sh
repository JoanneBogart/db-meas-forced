#!/usr/bin/env bash
# LSST Stack is required to run this script.

set -e

rootdir="$(mktemp -d)"

echo 'lsst.obs.hsc.HscMapper' > "$rootdir"/_mapper
makeSkyMap.py "$rootdir" --rerun=skymap

mv "$rootdir"/rerun/skymap/deepCoadd/skyMap.pickle .

echo "skyMap.pickle has been created in the current directory."
