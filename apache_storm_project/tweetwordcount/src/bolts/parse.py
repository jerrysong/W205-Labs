from __future__ import absolute_import, print_function, unicode_literals

import re
from streamparse.bolt import Bolt

################################################################################
# Function to check if the string contains only ascii chars
################################################################################
def ascii_string(s):
    return all(ord(c) < 128 for c in s)

def is_digit(char):
    return ord(char) > 47 and ord(char) < 58

def is_lowercase_letter(char):
    return ord(char) > 96 and ord(char) < 123

# Strip leading characters which are not digit or lowercase letter
def strip_leading(word):
    for index, char in enumerate(word):
        if is_digit(char) or is_lowercase_letter(char):
            return word[index:]
    return ''

# Strip lagging characters which are not digit or lowercase letter
def strip_lagging(word):
    word_len = len(word)
    for index, char in enumerate(reversed(word)):
        if is_digit(char) or is_lowercase_letter(char):
            return word[:word_len - index]
    return ''

class ParseTweet(Bolt):

    def process(self, tup):
        tweet = tup.values[0]  # extract the tweet

        # Split the tweet into words
        words = tweet.split()

        # Filter out the hash tags, RT, @ and urls
        valid_words = []
        for word in words:

            # Cast the word to lowercase
            word = word.lower()

            # Filter the hash tags
            if word.startswith("#"): continue

            # Filter the user mentions
            if word.startswith("@"): continue

            # Filter out retweet tags
            if word.startswith("rt"): continue

            # Filter out the urls
            if word.startswith("http"): continue

            # Cast the word to lowercase
            word = word.lower()

            # Strip leading and lagging characters which are not digit or letter
            aword = strip_leading(word)
            aword = strip_lagging(aword)

            # now check if the word contains only ascii
            if len(aword) > 0 and ascii_string(word):
                valid_words.append([aword])

        if not valid_words:
            return

        # Emit all the words
        self.emit_many(valid_words)

        # tuple acknowledgement is handled automatically
