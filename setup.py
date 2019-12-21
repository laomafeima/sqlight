from setuptools import setup
from sqlight import VERSION


def readme():
    with open('README.md') as f:
        return f.read()


setup(
    name="sqlight",
    version=VERSION,
    description="A lightweight wrapper around SQLite, MySQL, PostgreSQL.",
    long_description=readme(),
    long_description_content_type='text/markdown',
    keywords='sqlite sqlite3 mysql postgresql postgre lightweight wrapper',
    author='laoma',
    url='https://github.com/laomafeima/sqlight',
    license='http://www.apache.org/licenses/LICENSE-2.0',
    packages=["sqlight", "sqlight.platforms"],
    python_requires=">=3.5",
    project_urls={
        'Documentation': 'https://github.com/laomafeima/sqlight',
        'Source': 'https://github.com/laomafeima/sqlight',
    },
)
