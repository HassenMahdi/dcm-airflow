#!/usr/bin/python
# -*- coding: utf-8 -*-
import uuid
from datetime import datetime, timedelta


def generate_id():
    return uuid.uuid4().hex.upper()


def is_container(obj):
    """Test if an object is a container (iterable) but not a string"""
    return hasattr(obj, '__iter__') and not isinstance(obj, str)


def get_start_date(start_date):
    if not start_date:
        return (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return start_date