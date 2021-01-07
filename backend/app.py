#!/usr/bin/python
# -*- coding: utf-8 -*-

from api.utils.factory import create_app
import os

if __name__ == '__main__':
    app = create_app(os.getenv('APP_ENV', 'dev'))
    app.run(port=5006)

