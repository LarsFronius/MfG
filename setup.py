#!/usr/bin/env python

from distutils.core import setup

readme = open('README.txt').readlines()
description = readme[0].strip()
long_description ="".join(readme[2:]).strip()

setup(name='MfG',
        version='1.0',
        description=description,
        long_description=long_description,
        author='Lars Fronius',
        author_email='lars@jimdo.com',
        url='https://github.com/Jimdo/MfG',
        py_modules = ['munin'],
        scripts=['mfg'],
        )

