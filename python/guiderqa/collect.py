#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Created: 2018-06-11
# @LastModified: 2018-06-11
# @Filename: collect.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
# @Copyright: José Sánchez-Gallego

import os
import pathlib
import re

import astropy.io.fits
import astropy.table
import peewee
import playhouse.migrate
import playhouse.reflection
import tqdm

from . import log
from .db import models


type_field_mapping = {int: peewee.IntegerField,
                      float: peewee.FloatField,
                      str: peewee.CharField,
                      bool: peewee.BooleanField}


def walkdir(folder):
    """Walk through each files in a directory"""

    for dirpath, dirs, files in os.walk(folder):
        for filename in files:
            if not filename.startswith('proc-'):
                continue
            yield os.path.abspath(os.path.join(dirpath, filename))


def add_columns(database, table_name, payload={}):
    """Adds new columns to a table, in run-time.

    Parameter:
        database (`Database <http://docs.peewee-orm.com/en/latest/peewee/api.html#Database>`_):
            The peewee database to use.
        table_name (str):
            The name of the table.
        payload (dict):
            A mapping of key-value. All keys that are not present in the table
            will be added. The value will be used to determine the type of the
            new column.

    """

    migrator = playhouse.migrate.SqliteMigrator(database)

    colnames = [col.name for col in database.get_columns(table_name)]

    new_columns = []
    for key in payload:

        db_key = key
        value = payload[key]

        if db_key not in colnames:
            new_field = type_field_mapping[type(value)](null=True)
            new_columns.append(migrator.add_column(table_name, db_key, new_field))

    if len(new_columns) > 0:
        playhouse.migrate.migrate(*new_columns)
        return len(new_columns)
    else:
        return 0


def collect(path, mjd0, mjd1=None, outfile='guiderqa.db', split_db=False):
    """Collects guider data from a series of MJDs into a database.

    Parameters
    ----------
    path : str
        The path to the guider data. The guider images should be organised in
        directories under ``path``, one for each MJD.
    mjd0 : int
        The initial MJD to consider.
    mjd1 : int, optional
        The final MJD. If not defined, only ``mjd0`` will be collected.
    outfile : str, optional
        The name of the database file to write.
    split_db : bool, optional
        If True, each MJD creates its own database. The path of each database
        is ``outfile`` with a ``_{MJD}`` suffix.

    """

    if mjd1 is None:
        mjd1 = mjd0
        log.debug(f'setting final MJD to {mjd1}')

    path = pathlib.Path(path)
    assert path.exists(), f'guider path {path!s} not found'

    outfile = pathlib.Path(outfile)
    if outfile.exists():
        raise FileExistsError(f'database {outfile} exists.')

    if not split_db:
        models.database.init(str(outfile))
        models.database.connect()
        assert models.database.is_closed() is False, 'database cannot be opened.'

        # Inserts the tables. If they already exist they won't be replaced.
        models.database.create_tables([models.Frame, models.Header, models.BinTable], safe=True)

    # Computs number of files for the progress bar
    nfiles = 0
    proc_paths = []
    for mjd in range(mjd0, mjd1 + 1):
        path_mjd = path / f'{mjd}'
        if not path_mjd.exists():
            continue
        for file in walkdir(path_mjd):
            nfiles += 1
            proc_paths.append(file)

    for proc_path in tqdm.tqdm(proc_paths, total=nfiles):

        hdu = astropy.io.fits.open(proc_path)[0]
        hdu.verify('silentfix')
        header = hdu.header

        if header['IMAGETYP'] == 'dark':
            bintable = None
        else:
            bintable = astropy.table.Table.read(proc_path)

        mjd, frame = re.search('([0-9]*)/proc-gimg-([0-9]*)', str(proc_path)).groups()
        mjd = int(mjd)
        frame = int(frame)

        if split_db:
            suffix = outfile.suffix
            database_mjd = str(outfile).replace(suffix, f'_{mjd}{suffix}')

            if models.database.database != database_mjd:
                models.database.init(str(database_mjd))
                models.database.connect()
                assert models.database.is_closed() is False, 'database cannot be opened.'

                models.database.create_tables([models.Frame, models.Header, models.BinTable],
                                              safe=True)

        # Adds or retrieves the new frame.
        frame_dbo, __ = models.Frame.get_or_create(frame=frame, mjd=mjd, processed=True)

        # Adds new columns to Header
        header_values = {key.lower().replace('-', '_'): header[key]
                         for key in header if key.lower() != 'comment'}
        add_columns(models.database, 'header', header_values)

        # Gets the new header model from reflection
        full_header_model = playhouse.reflection.Introspector.from_database(
            models.database).generate_models()['header']

        with models.database.atomic():
            header_dbo = full_header_model.get_or_create(frame_pk=frame_dbo.pk, extension=0)[0]
            header_blob = bytes(header.tostring(), 'utf-8')
            full_header_model.update(header_blob=header_blob, **header_values).where(
                full_header_model.pk == header_dbo.pk).execute()

        if bintable is None:
            continue

        # Loads the bintable
        db_cols = models.database.get_columns('bintable')

        # Removes previous bintable row for this frame, just in case.
        with models.database.atomic():
            models.BinTable.delete().where(models.BinTable.frame_pk == frame_dbo.pk).execute()

        # Selects only columns that are in the model.
        bintable_db = bintable[[col.name for col in db_cols if col.name in bintable.colnames]]
        bintable_data = [{bintable_db.colnames[ii]: col for ii, col in enumerate(row)}
                         for row in bintable_db]

        [dd.update({'frame_pk': frame_dbo.pk}) for dd in bintable_data]  # Adds frame_pk
        with models.database.atomic():
            for data_dict in bintable_data:
                models.BinTable.create(**data_dict)

    return
