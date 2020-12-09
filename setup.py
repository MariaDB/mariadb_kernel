import os

from setuptools import find_packages, setup

with open('README.md') as f:
    readme = f.read()

setup(
    name='mariadb_kernel',
    packages=find_packages(),
    description='A simple MariaDB Jupyter kernel',
    long_description=readme,
    long_description_content_type="text/markdown",
    author='MariaDB Foundation',
    author_email='foundation@mariadb.org',
    url='https://github.com/MariaDB/mariadb_kernel',
    install_requires=open("requirements.txt").read().splitlines(),
    python_requires=">=3.5",
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
    ],
    use_scm_version={
        'version_scheme': 'guess-next-dev',
        'local_scheme': 'dirty-tag',
        'write_to': 'mariadb_kernel/_version.py'
    },
    setup_requires=['setuptools_scm'],
)
