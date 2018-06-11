# !usr/bin/env python
# -*- coding: utf-8 -*-
#
# Licensed under a 3-clause BSD license.
#
# @Author: Brian Cherinka
# @Date:   2017-12-05 12:01:21
# @Last modified by:   Brian Cherinka
# @Last Modified time: 2017-12-05 12:19:32

from __future__ import print_function, division, absolute_import


class GuiderQAError(Exception):
    """A custom core GuiderQA exception"""

    def __init__(self, message=None):

        message = 'There has been an error' \
            if not message else message

        super(GuiderQAError, self).__init__(message)


class GuiderQANotImplemented(GuiderQAError):
    """A custom exception for not yet implemented features."""

    def __init__(self, message=None):

        message = 'This feature is not implemented yet.' \
            if not message else message

        super(GuiderQANotImplemented, self).__init__(message)


class GuiderQAAPIError(GuiderQAError):
    """A custom exception for API errors"""

    def __init__(self, message=None):
        if not message:
            message = 'Error with Http Response from GuiderQA API'
        else:
            message = 'Http response error from GuiderQA API. {0}'.format(message)

        super(GuiderQAAPIError, self).__init__(message)


class GuiderQAApiAuthError(GuiderQAAPIError):
    """A custom exception for API authentication errors"""
    pass


class GuiderQAMissingDependency(GuiderQAError):
    """A custom exception for missing dependencies."""
    pass


class GuiderQAWarning(Warning):
    """Base warning for GuiderQA."""


class GuiderQAUserWarning(UserWarning, GuiderQAWarning):
    """The primary warning class."""
    pass


class GuiderQASkippedTestWarning(GuiderQAUserWarning):
    """A warning for when a test is skipped."""
    pass


class GuiderQADeprecationWarning(GuiderQAUserWarning):
    """A warning for deprecated features."""
    pass
