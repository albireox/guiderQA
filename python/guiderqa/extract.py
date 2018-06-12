#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Created: 2018-06-12
# @LastModified: 2018-06-12
# @Filename: extract.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
# @Copyright: José Sánchez-Gallego

import pathlib
import warnings

import numpy
import pandas
import playhouse.reflection
import tqdm

from .core import exceptions
from .db.models import Frame, database


def extract_header(mjds, path, keywords, dtypes=None, split_dbs=False, is_range=False):
    """Returns a `~pandas.DataFrame` with header information.

    For a list or range of MJDs, collects a series of header keywords for
    database files and organises them in a `~pandas.DataFrame` sorted by
    MJD and frame number.

    Parameters
    ----------
    mjds : list
        The list of MJDs to extract. If the lenght of ``mjds`` is two and
        ``is_range=True``, all the MJDs between both values will be extracted.
    path : str
        The path to the database file.
    keywords : list
        A list of strings with the header keywords to extract.
    dtypes : list, optional
        A list of types to cast the keyword values.
    split_dbs : bool, optional
        If True, assumes that the DB is split into multiple files, one for each
        MJD. In that case, the path for each file is assumed to be ``path``
        with the ``_{MJD}`` suffix.
    is_range : bool, optional
        If True, assumes that ``mjds`` are the extremes of a range of MJDs.

    """

    mjds = numpy.atleast_1d(mjds)
    path = pathlib.Path(path)

    keywords = [key.lower() for key in keywords]

    if dtypes:
        assert len(dtypes) == len(keywords), 'inconsistent lenghts of keywords and dtypes'

    assert mjds.ndim == 1, 'invalid number of dimensions in mjds'

    if is_range:
        assert len(mjds) == 2, 'when is_range=True, mjds must be a list of lenght 2'
        mjds = numpy.arange(mjds[0], mjds[1] + 1)

    if not split_dbs:
        assert path.exists()
        database.init(str(path))
        assert database.connect(), 'cannot connect to database'

    dataframes = []

    with tqdm.trange(len(mjds)) as tt:

        for mjd in mjds:
            tt.set_description(str(mjd))

            if split_dbs:
                suffix = path.suffix
                database_mjd = str(path).replace(suffix, f'_{mjd}{suffix}')
                if not pathlib.Path(database_mjd).exists():
                    tt.update()
                    continue
                database.init(str(database_mjd))
                assert database.connect(), 'cannot connect to database'

            Header = playhouse.reflection.Introspector.from_database(
                database).generate_models()['header']

            fields = [Frame.mjd, Frame.frame]

            failed = any([not hasattr(Header, keyword) for keyword in keywords])
            if failed:
                tt.update()
                continue

            for keyword in keywords:
                fields.append(getattr(Header, keyword))

            data = Header.select(*fields).join(Frame, on=(Frame.pk == Header.frame_pk)).tuples()
            dataframes.append(pandas.DataFrame(list(data), columns=(['mjd', 'frame'] + keywords)))

            tt.update()

    dataframe = pandas.concat(dataframes)

    if dtypes:
        failed = False
        for ii, key in enumerate(keywords):
            try:
                dataframe[key] = dataframe[key].astype(dtypes[ii])
            except ValueError as ee:
                warnings.warn(f'failed to apply astype: {ee!r}', exceptions.GuiderQAUserWarning)
                failed = True

        if not failed:
            dataframe = dataframe[dataframe > -999.]

    # dataframe = dataframe.orderby(['mjd', 'frame'])
    dataframe = dataframe.set_index(['mjd', 'frame'])
    dataframe.sort_index(inplace=True)

    return dataframe
