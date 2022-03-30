#!/usr/bin/python
# -*- coding: utf-8 -*-

from api.utils.factory import create_app
import os

app = create_app(os.getenv('APP_ENV', 'dev'))

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000)

