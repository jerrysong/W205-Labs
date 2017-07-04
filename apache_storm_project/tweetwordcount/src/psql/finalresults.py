import sys
from psql.psqlclient import PsqlClient

def print_one_entry_info(word):
    """Print the count of a given word in the database."""
    psql_client = PsqlClient()
    count = psql_client.get_count(word)
    psql_client.close()
    _print(word, count)

def print_all_entries_info():
    """Print the count of every word in the database."""
    psql_client = PsqlClient()
    words = psql_client.fetch_all_in_order()
    for word, count in words:
        _print(word, count)

def _print(word, count):
    print 'Total number of occurences of "{0}": {1}'.format(word, count)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print_all_entries_info()
    else:
        for word in sys.argv[1:]:
            print_one_entry_info(word)