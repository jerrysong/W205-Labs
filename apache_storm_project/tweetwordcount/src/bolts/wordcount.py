from __future__ import absolute_import, print_function, unicode_literals

from collections import Counter
from streamparse.bolt import Bolt

from psql.psqlclient import PsqlClient

class WordCounter(Bolt):

    def initialize(self, conf, ctx):
        self.local_counts = Counter()
        self.psql_client = PsqlClient()

    def process(self, tup):
        word = tup.values[0]

        # Increment the local count
        self.local_counts[word] += 1
        self.emit([word, self.local_counts[word]])

        # Log the count to the console
        self.log('%s: %d' % (word, self.local_counts[word]))

        # Get the count of the word from the database
        existing_count = self.psql_client.get_count(word)

        # Insert the word to the database if it is new
        if existing_count == 0:
            self.psql_client.insert(word)
        # Increase the count of the word by 1 if it already exists
        else:
            self.psql_client.update(word, existing_count + 1)
            
    def __del__(self):
        # Close the psql connection when the bolt work ends
        self.psql_client.close()

