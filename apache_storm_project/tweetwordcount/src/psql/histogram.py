import sys
from psql.psqlclient import PsqlClient

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print 'You must provide both lower count limit and upper count limit'
        sys.exit()

    try:
        low = int(sys.argv[1])
        high = int(sys.argv[2])
    except ValueError, e:
        print 'Both lower count limit and upper count limit must be integer'
        sys.exit()

    psql_client = PsqlClient()
    words = psql_client.fetch_range(low, high)
    for word, count in words:
        print 'Total number of occurences of "{0}": {1}'.format(word, count)