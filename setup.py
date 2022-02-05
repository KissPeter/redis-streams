#!/usr/bin/env python3
"""
Preparation instructions: https://packaging.python.org/tutorials/packaging-projects/
"""
import os.path
import re

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
    open("redis_streams/__init__.py").read(),
).group(1)


def get_readme():
    readme = list()
    with open("README.md", "r") as f:
        skip_lines = True
        for line in f.read().splitlines():
            if line.startswith('# Redis-Streams'):
                skip_lines = False
            if line.startswith('### Runnel'):
                skip_lines = True
            if skip_lines:
                continue
            else:
                readme.append(line)
    return '\n'.join(readme)


def get_requirements():
    requirements = list()
    with open(os.path.join(here, "requirements.txt"), "r") as f:
        for line in f.read().splitlines():
            if not line.startswith("#") and not line.startswith("--"):
                requirements.append(line)
    return '\n'.join(requirements)


setup_options = dict(
    name='redis-streams',
    version=__version__,
    description="Redis Streams client implementation for high availability usage "
                "including consumer, monitor and scaler implementation",
    long_description=get_readme(),
    long_description_content_type="text/markdown",
    author='Peter Kiss',
    author_email='peter.kiss@linuxadm.hu',
    url='https://github.com/KissPeter/redis-streams/',
    project_urls={
        "Documentation": "https://github.com/KissPeter/redis-streams",
        "Changes": "https://github.com/KissPeter/redis-streams//releases",
        "Code": "https://github.com/KissPeter/redis-streams",
        "Issue tracker": "https://github.com/KissPeter/redis-streams//issues",
    },
    scripts=[],
    packages=find_packages(exclude=["redis_streams_test"]),
    install_requires=get_requirements(),
    license="GNU General Public License v3.0",
    classifiers=[  # https://pypi.org/classifiers/
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Communications',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Distributed Computing',
        'Natural Language :: English',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
    ],
    python_requires='>=3.6, <4',
    package_data={"redis_streams": ['*.py']},
    exclude_package_data={"redis_streams_test": ["*"]}
)

setup(**setup_options)
