''' This program extracts fields from the electronic report files (*.fec)

import csv
import re
import pprint

pp = pprint.PrettyPrinter(indent=4)




with open('headers-all.csv', 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')