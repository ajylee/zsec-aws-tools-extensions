import setuptools

setuptools.setup(
    name='zsec-aws-tools-extensions',
    python_requires='>=3.8',
    packages=['zsec_aws_tools_extensions'],
    install_requires=[
        'boto3',
        'toolz',
        'attrs',
        'zsec-aws-tools >= 0.1.20',
    ],
    tests_require=[
        'toolz',
        'pytest',
    ],
    version='0.1.7',
)
