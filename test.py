import datetime
single_date = datetime.date(2023, 4, 6)

print(single_date == datetime.date(int('2023-04-06'.split('-')[0]), int('2023-04-06'.split('-')[1]), int('2023-04-06'.split('-')[2])))