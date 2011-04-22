#! /usr/bin/env python

from mrjob.job import MRJob

class CharacterCount(MRJob):
    def mapper(self, _, text):
        for c in text:
            yield c, 1

    def reducer(self, c, counts):
        yield c, sum(counts)

if __name__ == '__main__':
    CharacterCount.run()
