#!/usr/bin/env python
import uuid
import random
import csv

u = ['user-{}'.format(uuid.uuid4()) for i in range(3)]
pages = ['/usr/home', '/usr/videos/264982164', '/product/iphone5', '/product/galaxys4', '/payment/makepayment']
events = ['click', 'drag', 'hover', 'dblclick']
els = ['a#save', 'button#submit', 'div.color_palette', 'a#cancel', 'a#block', 'button#confirm_ok']

rows = []
for i in range(100):
  row = [
      random.choice(u),
      random.choice(pages),
      random.choice(events),
      random.choice(els)
      ]
  rows.append(row)

filename = 'events.csv'
with open(filename, 'wb') as csvf:
  writer = csv.writer(csvf)
  for row in rows:
    writer.writerow(row)

print 'Generation of random events finished. Please see file: {}'.format(filename) 
