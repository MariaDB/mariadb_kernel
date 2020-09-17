import os

from setuptools import find_packages, setup

with open('README.md') as f:
    readme = f.read()

# Get the current package version.
here = os.path.abspath(os.path.dirname(__file__))
version_ns = {}
with open(os.path.join(here, 'mariadb_kernel', '_version.py')) as f:
        exec(f.read(), {}, version_ns)

setup(
    name='mariadb_kernel',
    version=version_ns['__version__'],
    packages=find_packages(),
    description='A simple MariaDB Jupyter kernel',
    long_description=readme,
    author='MariaDB Foundation',
    author_email='foundation@mariadb.org',
    url='https://github.com/MariaDB/mariadb_kernel',
    install_requires=open("requirements.txt").read().splitlines(),
    python_requires=">=3.5",
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
    ],
)
