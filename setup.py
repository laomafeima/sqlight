from setuptools import setup
from sqlight import version

setup(
    name="sqlight",
    version=version,
    description="A lightweight wrapper around SQLite.",
    long_description='A lightweight wrapper around SQLite. just on Python3',
    keywords='sqlite sqlite3 lightweight wrapper',
    author='laoma',
    url='https://github.com/laomafeima/sqlight',
    license='http://www.apache.org/licenses/LICENSE-2.0',
    py_modules=["sqlight"],
    )
