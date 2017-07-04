import psycopg2

class PsqlClient():

    """Client class for interacting with the postgres"""

    # The default db name for this project
    DATABASE = 'tcount'
    # The default psql user for this project
    USER = 'postgres'

    def __init__(self):
        self.conn = psycopg2.connect(database = self.DATABASE, user = self.USER)
        self.cur = self.conn.cursor()

    def update(self, word, count):
        """Update the count of an existing word."""
        self.cur.execute('UPDATE tweetwordcount SET count = %s WHERE word = %s', (count, word))
        self.conn.commit()

    def insert(self, word):
        """Insert a new word with count 1."""
        self.cur.execute('INSERT INTO tweetwordcount (word, count) VALUES (%s, %s)', (word, 1))
        self.conn.commit()

    def get_count(self, word):
        """Return the count of the given word in the db if it exists, otherwise return 0."""
        # This query should return an None if the given word doesn't exist
        self.cur.execute('SELECT count FROM tweetwordcount WHERE word = %s', (word,))
        res = self.cur.fetchone()

        if res is None:
            return 0
        else:
            return res[0]

    def fetch_all_in_order(self):
        """Fetch all entries in the db and return them in ascending order."""
        self.cur.execute('SELECT * FROM tweetwordcount ORDER BY word')
        res = self.cur.fetchall()
        return res

    def fetch_range(self, low, high):
        """Fetch all entires whose count is within the interval [low, high] and return them in ascending order of count"""
        self.cur.execute('SELECT * FROM tweetwordcount WHERE count >= %s and count <= %s ORDER BY count', (low, high))
        res = self.cur.fetchall()
        return res

    def close(self):
        """Close the connection with the psql database."""
        self.conn.close()
