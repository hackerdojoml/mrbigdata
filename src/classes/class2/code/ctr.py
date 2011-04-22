#! /usr/bin/env python
from __future__ import division

from mrjob.job import MRJob

class CalculateCTR(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json_value'

    def mapper(self, _, ad_event):
        if ad_event['billable_impression']:
            user_clicked = ad_event['billable_click']
            yield None, user_clicked

    def reducer(self, _, events):
        impression_count = 0
        click_count = 0
        for event in events:
            impression_count += 1
            if event:  # i.e., it was clicked
                click_count += 1

        yield None, click_count / impression_count


if __name__ == '__main__':
    CalculateCTR.run()
