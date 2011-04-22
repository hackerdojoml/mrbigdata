#! /usr/bin/env python

from mrjob.job import MRJob

class WordCount(MRJob):
    def mapper(self, _, text):
        for word in text.split():
            yield word, 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == '__main__':
    WordCount.run()
