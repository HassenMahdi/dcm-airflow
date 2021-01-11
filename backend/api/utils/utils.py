#!/usr/bin/python
# -*- coding: utf-8 -*-
import uuid


def generate_id():
    return uuid.uuid4().hex.upper()
