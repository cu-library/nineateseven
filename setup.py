from setuptools import setup

setup(
    name='nineateseven',
    version='0.1',
    py_modules=['nineateseven'],
    install_requires=[
        'Click',
    ],
    entry_points='''
        [console_scripts]
        nineateseven=nineateseven:cli
    ''',
)
