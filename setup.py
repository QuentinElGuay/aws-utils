from setuptools import find_packages, setup

setup(
    name='aws-utils',
    packages=find_packages(include=['awsutils']),
    version='0.1.0',
    description='Usefull tools to use tAWS services.',
    author='Quentin Leguay',
    license='GPLv3',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest~=7.1.2'],
    test_suite='tests',
)