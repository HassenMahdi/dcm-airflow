#!/usr/bin/python
# -*- coding: utf-8 -*-
import uuid


def generate_id():
    return uuid.uuid4().hex.upper()


def is_container(obj):
    """Test if an object is a container (iterable) but not a string"""
    return hasattr(obj, '__iter__') and not isinstance(obj, str)

