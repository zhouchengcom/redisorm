#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import re

from setuptools import setup


with open(os.path.join(os.path.dirname(__file__), 'redisorm/__init__.py')) as f:
    version = re.search("^__version__ = '(\d\.\d+\.\d+((\.dev|a|b|rc)\d?)?)'$",
                        f.read(), re.M).group(1)

setup(
    name='redisorm',
    license='BSD',
    version=version,
    description='Python orm for redis',
    author=u'Zhoucheng',
    author_email='chengzcom0926@gmail.com',
    packages=['redisorm', 'redisorm.types'],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    install_requires=["schematics"],
)
