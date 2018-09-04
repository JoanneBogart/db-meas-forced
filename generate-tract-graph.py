#!/usr/bin/env python

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

"""
Generate 'tractGraph.pickle' from 'skyMap.pickle'.

tractGraph is a graph(V,E) whose vertices(V) are tracts.
That an edge(E) exists between two vertices means
that the two tracts are adjacent.

(i,j) \in E <=> 'j in tractGraph[i]'
"""

import numpy

import pickle
import sys


skyMapPath, = sys.argv[1:]
skyMap = pickle.load(open(skyMapPath, "rb"))
nTracts = len(skyMap)

print("Creating nodes (i.e. 'tracts') from skyMap...")
node_center = numpy.empty(shape=(nTracts, 3))
node_radius = numpy.empty(shape=(nTracts,))

for i, tract in enumerate(skyMap):
    center = numpy.array(tract.getCtrCoord().getVector(), dtype=float)
    vertexes = [numpy.array(vertex.getVector(), dtype=float) for vertex in tract.getVertexList()]
    radius = numpy.arccos(min(numpy.dot(center, vertex) for vertex in vertexes))

    node_center[i] = center
    node_radius[i] = radius

node_radius = node_radius.reshape(nTracts, 1)

print("Computing distances between nodes...")
distance_btw_nodes = numpy.dot(node_center, node_center.T)
numpy.clip(distance_btw_nodes, -1.0, 1.0, out=distance_btw_nodes)
numpy.arccos(distance_btw_nodes, out=distance_btw_nodes)

print("Determining whether node i and node j are adjacent or not...")
is_adjacent = (distance_btw_nodes < node_radius + node_radius.T)

del distance_btw_nodes

print("Creating edges between adjacent nodes...")
graph = []

for i in range(nTracts):
    graph.append(
        set(numpy.nonzero(is_adjacent[i])[0])
    )

pickle.dump(graph, open("tractGraph.pickle", "wb"))
