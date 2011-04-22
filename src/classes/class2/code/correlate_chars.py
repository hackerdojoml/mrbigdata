#! /usr/bin/env python

from mrjob.job import MRJob

class CorrelateChars(MRJob):
    def mapper(self, _, line):
        words = line.lower().split()
        for word in words:
                for char1, char2 in zip(word[:-1], word[1:]):
                        yield (char1, char2), 1

    def reducer(self, char_pair, counts):
        total = sum(counts)
        if total > 5:
                yield char_pair, total

if __name__ == '__main__':
    CorrelateChars.run()
