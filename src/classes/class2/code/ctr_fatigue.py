#! /usr/bin/env python
from __future__ import division

from constants import obscure_id, overall_ctr

from mrjob.job import MRJob

class CalculateCTR(MRJob):
    DEFAULT_INPUT_PROTOCOL = 'json_value'

    def map_to_ad_event_by_user(self, _, ad_event):
        if ad_event['billable_impression']:
            user = ad_event['opportunity']['unique_visitor_id']
            ad_timestamp = ad_event['opportunity']['ad_delivery_end_time']
            user_clicked = ad_event['billable_click']
            yield user, (ad_timestamp, user_clicked)

    def reduce_to_click_by_position(self, user, events):
        i = 0
        for _, user_clicked in sorted(events):
            yield i, user_clicked
            i += 1

    def map_to_click_by_position(self, position, user_clicked):
        yield position, user_clicked

    def reduce_to_ctr_by_position(self, position, events):
        impression_count = 0
        click_count = 0
        for event in events:
            impression_count += 1
            if event:  # i.e., it was clicked
                click_count += 1

        ctr = click_count / impression_count
        yield position, ctr / overall_ctr

    def steps(self):
        return [
            self.mr(
                mapper=self.map_to_ad_event_by_user,
                reducer=self.reduce_to_click_by_position
            ),
            self.mr(
                mapper=self.map_to_click_by_position,
                reducer=self.reduce_to_ctr_by_position
            ),
        ]


if __name__ == '__main__':
    CalculateCTR.run()
