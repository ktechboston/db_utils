import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="db_utils",
    version="0.4.6",
    author="Komodo Technologies, LLC",
    description="Helper class to connect to Redshift, Snowflake, DynamoDB and S3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ktechboston/db_utils",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
    install_requires=[
        'boto3>=1.5.32',
        'psycopg2>=2.8.2',
        'psycopg2-binary>=2.7.4'
        'numpy>=1.16.3',
        'pandas==0.24.2',
        'sqlparse>=0.2.4',
        'awscli>=1.16.32',
        'snowflake-connector-python>=1.7.3',
        'mysql-connector-python>=8.0.15',
        'pyodbc>=4.0.26',
        'jinja2>=2.10.1'
    ],
)
