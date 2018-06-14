#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Created: 2018-06-12
# @LastModified: 2018-06-12
# @Filename: analysis.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
# @Copyright: José Sánchez-Gallego

import astropy
import numpy
import pandas


def groupby_range(dataframe, range_column, value_column, bins=5, index_column='mjd',
                  aggfunc=numpy.median):
    """Groups data by range and extracts a column.

    Parameters
    ----------
    dataframe : `pandas.DataFrame`
        The input DataFrame.
    range_column : str
        The column to cut in ranges.
    value_column : str
        The column to extract.
    bins : list or int, optional
        A valid bins parameter for pandas.cut`.
    index_column : str, optional
        The index column to do the initial group by.
    aggfunc : function, optional
        A function to be passed to `pandas.pivot_table` to aggregate the data.

    """

    dataframe = dataframe.copy()

    dataframe.reset_index(inplace=True)

    df_cut = pandas.cut(dataframe[range_column], bins)
    dataframe['ranges'] = df_cut

    dataframe = dataframe.dropna(subset=['ranges'])

    columns = [index_column] + [value_column, 'ranges']
    df = dataframe[columns]

    return pandas.pivot_table(df, values=value_column, index=index_column, columns=['ranges'],
                              aggfunc=aggfunc)


def get_ha(dataframe, ra_column='ra', obstime_column='date_obs', observatory='apo'):
    """Returns a `~pandas.Series` with the HA information for an observation.

    Parameters
    ----------
    dataframe : pandas.DataFrame
        The DataFrame containing the data.
    ra_column : str, optional
        The name of the column holding the R.A. values.
    obstime_column : str, optional
        The name of the column holding the observed time values. The values
        must be in the form of an ISO timestamp.
    observatory : str, optional
        Either ``'apo'`` or ``'lco'``.

    """

    obs = astropy.coordinates.EarthLocation.of_site(observatory.lower())
    times = astropy.time.Time(list(dataframe[obstime_column].values), location=obs)
    local_sidereal = times.sidereal_time('mean')

    ra = dataframe[ra_column]
    ha = local_sidereal.deg - ra

    return ha
