import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="db_utils",
    version="0.1.7",
    author="Komodo Technologies, LLC",
    author_email="flora@ktechboston.com",
    description="help access SQL",
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
        'boto3',
        'psycopg2',
        'numpy',
        'pandas',
        'sqlparse',
        'awscli'
    ],
)
