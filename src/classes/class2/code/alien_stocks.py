#! /usr/bin/env python
from __future__ import division

from mrjob.job import MRJob

states = '''
	AL AK AS AZ AR CA CO CT DE DC FM FL GA GU HI
	ID IL IN IA KS KY LA ME MH MD MA MI MN MS MO
	MT NE NV NH NJ NM NY NC ND MP OH OK OR PW PA
	PR RI SC SD TN TX UT VT VI VA WA WV WI WY
'''
states = set(states.split())

class AlienStocks(MRJob):
	DEFAULT_INPUT_PROTOCOL = 'json_value'

	def mapper(self, _, row):
		if 'stock_symbol' in row:
			date = row['date']
			opening_price = float(row['stock_price_open'])
			closing_price = float(row['stock_price_close'])
			volume = int(row['stock_volume'])
			market_cap_change = volume * (closing_price - opening_price)
			market_cap_change = int(round(market_cap_change))
			yield date, ('stock', market_cap_change)
		else:
			date = row['sighted']
			location = row['location'][-2:]
			if location in states:
				yield date, ('alien', location)

	def reducer(self, date, rows):
		market_cap_change = 0
		locations = set()
		for row_type, row_data in rows:
			if row_type == 'stock':
				market_cap_change += row_data
			else:  # alien
				locations.add(row_data)
		for location in locations:
			yield location, market_cap_change

	def summing_reducer(self, location, market_cap_change):
		yield location, sum(market_cap_change)

	def steps(self):
		return [
			self.mr(
				mapper=self.mapper,
				reducer=self.reducer
			),
			self.mr(
				reducer=self.summing_reducer
			),
		]


if __name__ == '__main__':
	AlienStocks.run()
