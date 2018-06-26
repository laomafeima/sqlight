from setuptools import setup
from sqlight import version


def readme():
    with open('README.md') as f:
        return f.read()


setup(
    name="sqlight",
    version=version,
    description="A lightweight wrapper around SQLite.",
    long_description=readme(),
    keywords='sqlite sqlite3 lightweight wrapper',
    author='laoma',
    url='https://github.com/laomafeima/sqlight',
    license='http://www.apache.org/licenses/LICENSE-2.0',
    py_modules=["sqlight"],
    python_requires=">=3.5",
    )
