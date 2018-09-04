db-meas-forced
=====================

This is a collection of scripts to load HSC catalogs onto PostgreSQL.

This version has been adapted as needed for use with LSST from the
original written by Sogo Mineo for HSC

Requirements
----------------------

 * python >= 3.6
 * PostgreSQL >= 10
 * hscPipe 6.x

Prepare
----------------------

 1. Download [extinction module](https://hsc-gitlab.mtk.nao.ac.jp/sogo.mineo/extinction)
    and set `PYTHONPATH=/path/to/extinction/python` .

 2. Download [PostgreSQL cmath module](https://hsc-gitlab.mtk.nao.ac.jp/sogo.mineo/postgres-cmath)
    and install it in the DB.

 3. Install `postgres-objcatalog` by `cd postgres-objcatalog; make; make install` .

 4. Feed `preparation.sql` to DB to define necessary types and functions.

 5. Execute `create-table-skymap.py` to create table `skymap` .

 6. Execute `generate-skymap-wcs.py` to create WCS files in the current directory.
    These files are required in loading catalogs without corresponding image files.

 7. Execute `generate-tract-graph.py` to create `tractGraph.pickle` .
    This file is required in creating field search functions.

Load catalogs
----------------------

Execute `create-table-forced.py` , and `create-table-meas.py` .

Create indices
-----------------------------

Execute `create-table-forced.py` , and `create-table-meas.py`
with `--create-index` option.

The process of index creation is separated from catalog loading
so that you can load catalogs incrementally by calling `create-table-*.py`
several times before finally calling them with `--create-index` option.
Though it is still possible to do an incremental load after creating indices,
`create-table-*.py` will drop all indices before start loading since indices
are hindrance to row insertion.

Create field search functions
------------------------------------

Execute `generate-field-searches.py` . The generated search functions
will be output to stdout, which must be piped to `psql`.

Technical notes
--------------------

  * PostgreSQL's planner can expand function calls in line,
    if no hindrance exists. For instance, 'STRICT' modifier
    (which means 'always returns NULL when any of arguments
    are NULL) is a hindrance to inline expansion.

  * When function calls are inline-ed, the planner can use indices
    to perform the function bodies.

  * PostgreSQL's planner can ignore joined tables when possible.
    Relying on this optimization, we can join many tables in advance
    to make a view.  However, we have to think carefully whether a joined
    table can logically be ignored: otherwise, unnecessary tables won't
    be ignored in spite of our intention.

  * Be careful to avoid TOAST. TOAST is the vilest corruption in PostgreSQL
    if we are to make a big database with it. Not only would it spoil
    insertion performance, but TOAST would also cause an infinite loop
    (See the definition of GetNewOidWithIndex()) in insertion. It means a
    COPY TO query would last a few weaks (for the spoiled performance by
    TOAST), and the query may not finish forever trapped in the infinite loop.
    Little hope it will be fixed.

  * To avoid TOAST, we must not store data in arrays. We should also keep
    row sizes (in bytes) small.
