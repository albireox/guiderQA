#!/usr/bin/env python
# encoding: utf-8
#
# @Author: José Sánchez-Gallego
# @Date: Oct 15, 2017
# @Filename: models.py
# @License: BSD 3-Clause
# @Copyright: José Sánchez-Gallego


from __future__ import absolute_import, division, print_function

import astropy.io.fits
import peewee


database = peewee.SqliteDatabase(None)  # Un-initialized database.


class BaseModel(peewee.Model):
    class Meta:
        database = database


class Frame(BaseModel):
    """Represents a guider frame/image."""

    class Meta:
        db_table = 'frame'

    pk = peewee.PrimaryKeyField(primary_key=True)
    mjd = peewee.IntegerField(null=False)
    frame = peewee.IntegerField(null=False)
    processed = peewee.BooleanField(null=False)


class Header(BaseModel):
    """Represents a primary header."""

    class Meta:
        db_table = 'header'

    pk = peewee.PrimaryKeyField(primary_key=True)
    extension = peewee.IntegerField(default=0, null=False)
    header_blob = peewee.BlobField(null=True)

    frame = peewee.ForeignKeyField(Frame, db_column='frame_pk', null=False,
                                   related_name='header')

    def to_astropy(self):
        """Returns and astropy Header object from the blob."""

        return astropy.io.fits.Header.fromstring(self.header_blob.decode('utf-8'))


class BinTable(BaseModel):
    """Represents a bintable."""

    class Meta:
        db_table = 'bintable'

    pk = peewee.PrimaryKeyField(primary_key=True)
    frame = peewee.ForeignKeyField(Frame, db_column='frame_pk', null=False,
                                   related_name='bintable')

    exists = peewee.BooleanField(null=True)
    enabled = peewee.BooleanField(null=True)
    gprobebits = peewee.IntegerField(null=True)
    ra = peewee.FloatField(null=True)
    dec = peewee.FloatField(null=True)
    phi = peewee.FloatField(null=True)
    xFocal = peewee.FloatField(null=True)
    yFocal = peewee.FloatField(null=True)
    radius = peewee.FloatField(null=True)
    xFerruleOffset = peewee.FloatField(null=True)
    yFerruleOffset = peewee.FloatField(null=True)
    rotation = peewee.FloatField(null=True)
    rotStar2Sky = peewee.FloatField(null=True)
    focusOffset = peewee.FloatField(null=True)
    fiber_type = peewee.CharField(null=True)
    ref_mag = peewee.FloatField(null=True)
    fiberid = peewee.IntegerField(null=True)
    xCenter = peewee.FloatField(null=True)
    yCenter = peewee.FloatField(null=True)
    xFlex = peewee.FloatField(null=True)
    yFlex = peewee.FloatField(null=True)
    xstar = peewee.FloatField(null=True)
    ystar = peewee.FloatField(null=True)
    dx = peewee.FloatField(null=True)
    dy = peewee.FloatField(null=True)
    dRA = peewee.FloatField(null=True)
    dDec = peewee.FloatField(null=True)
    fwhm = peewee.FloatField(null=True)
    flux = peewee.FloatField(null=True)
    mag = peewee.FloatField(null=True)
    sky = peewee.FloatField(null=True)
    skymag = peewee.FloatField(null=True)
    poserr = peewee.FloatField(null=True)
    stampSize = peewee.IntegerField(null=True)
    stampIdx = peewee.IntegerField(null=True)
