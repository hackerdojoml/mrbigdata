#! /usr/bin/env python
from __future__ import division

from constants import obscure_id, overall_ctr

from mrjob.job import MRJob

class CalculateCTR(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json_value'

    def mapper(self, _, ad_event):
        if ad_event['billable_impression']:
            ad_campaign = ad_event['impression']['candidate_id']
            ad_type = ad_event['impression']['ad_type']
            user_clicked_through = ad_event['billable_click']
            yield (obscure_id(ad_campaign), ad_type), user_clicked
            yield (obscure_id(ad_campaign), 'TOTAL'), user_clicked
            yield ('TOTAL', ad_type), user_clicked
            yield 'TOTAL', user_clicked

    def reducer(self, grouping, events):
        impression_count = 0
        click_count = 0
        for event in events:
            impression_count += 1
            if event:  # i.e., it was clicked
                click_count += 1

        ctr = click_count / impression_count

        if ctr > 0:
            yield grouping, ctr / overall_ctr


if __name__ == '__main__':
    CalculateCTR.run()
