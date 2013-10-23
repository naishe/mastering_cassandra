#! /usr/bin/env python


from pycassa.pool import ConnectionPool
import csv

#class Util:
def getConnection():
  return ConnectionPool('mastering_cassandra', ['localhost:9160'])

def readCSV(filename):
  rows=[]
  with open(filename, 'rb') as csvf:
    reader = csv.reader(csvf, skipinitialspace=True)
    for row in reader:
      rows.append(row)
  return rows

if __name__ == '__main__':
  readCSV('data/movies')
