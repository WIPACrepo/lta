#!/usr/bin/env python

import os
import sys
import glob

if sys.version_info < (3, 6):
    print('ERROR: LTA requires at least Python 3.6+ to run.')
    sys.exit(1)

try:
    # Use setuptools if available, for install_requires (among other things).
    import setuptools
    from setuptools import setup
except ImportError:
    setuptools = None
    from distutils.core import setup

kwargs = {}

current_path = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(current_path, 'lta', '__init__.py')) as f:
    for line in f.readlines():
        if '__version__' in line:
            kwargs['version'] = line.split('=')[-1].split('\'')[1]
            break
    else:
        raise Exception('cannot find __version__')

with open(os.path.join(current_path, 'README.md')) as f:
    kwargs['long_description'] = f.read()

if setuptools is not None:
    # If setuptools is not available, you're on your own for dependencies.
    install_requires = [
        'coverage>=4.4.2',
        'PyJWT',
        'pymongo',
        'requests',
        'requests_toolbelt',
        'requests-futures',
        'sphinx>=1.4',
        'tornado>=5.1'
    ]
    kwargs['install_requires'] = install_requires
    kwargs['zip_safe'] = False

setup(
    name='lta',
    scripts=glob.glob('bin/*'),
    packages=['lta'],
    package_data={
        # data files need to be listed both here (which determines what gets
        # installed) and in MANIFEST.in (which determines what gets included
        # in the sdist tarball)
        # 'iceprod.server':['data/etc/*','data/www/*','data/www_templates/*'],
    },
    author="IceCube Collaboration",
    author_email="developers@icecube.wisc.edu",
    url="https://github.com/WIPACrepo/lta",
    license="https://github.com/WIPACrepo/lta/blob/master/LICENSE",
    description="LTA is the Long Term Archive service and related tools, developed for the IceCube Collaboration.",
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        'Operating System :: POSIX :: Linux',
        'Topic :: System :: Distributed Computing',

        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
    **kwargs
)
