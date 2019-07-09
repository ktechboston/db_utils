import os
import numpy as np
import pandas as pd
import sqlite3
import sqlparse
import configparser
import boto3
import json
import mysql.connector
from pprint import pprint
from decimal import Decimal
from boto3.dynamodb.types import TypeSerializer, TypeDeserializer
from botocore.config import Config
import psycopg2
import psycopg2.extras
import pyodbc


default_path = os.path.expanduser('~/.databases.conf')