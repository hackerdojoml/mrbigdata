#! /usr/bin/env python
from __future__ import division

from constants import obscure_id, overall_ctr

from mrjob.job import MRJob

class CalculateCTR(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json_value'

    def mapper(self, _, ad_event):
        if ad_event['billable_impression']:
            user = ad_event['opportunity']['unique_visitor_id']
            ad_timestamp = ad_event['opportunity']['ad_delivery_end_time']
            user_clicked = ad_event['billable_click']
            yield user, (ad_timestamp, user_clicked)

    def reducer(self, user, events):
        i = 0
        for _, user_clicked in sorted(events):
            yield i, user_clicked
            i += 1


if __name__ == '__main__':
    CalculateCTR.run()
