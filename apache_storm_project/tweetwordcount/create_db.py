"""This python script will create the needed database and table.
Please run the script prior to starting any process.
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Connect to the default database
conn = psycopg2.connect(database="template1", user="postgres")
cur = conn.cursor()

# To avoid "transaction block" error
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

# Drop the Tcount database if it already exists
sql_command = 'DROP DATABASE IF EXISTS tcount'
cur.execute(sql_command)

# Create the Tcount database
sql_command = 'CREATE DATABASE tcount;'
cur.execute(sql_command)
conn.close()

# Connect to the Tcount database
conn = psycopg2.connect(database="tcount", user="postgres")
cur = conn.cursor()

# Create the Tweetwordcount table and commit the changes
sql_command = '''
                 CREATE TABLE tweetwordcount (
                     word  TEXT PRIMARY KEY NOT NULL,
                     count INT              NOT NULL
                 );
              '''
cur.execute(sql_command)
conn.commit()
conn.close()

print 'Create db tcount and table tweetwordcount successfully'