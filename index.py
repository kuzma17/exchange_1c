#!/usr/bin/env python
# -*- coding: utf-8 -*-

from function import Exchange1c
import csv
import time
import os

ex1c = Exchange1c()
ex1c.open()

def csv_writer(data, path):
    with open(path, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=ex1c.delimiter)
        for line in data:
            writer.writerow(line)

def updateUser(path):
    with open(path, "r") as file_obj:
        reader = csv.reader(file_obj, delimiter=ex1c.delimiter)
        for row in reader:
            if row[0]:
                print 'update'
                ex1c.updateUser(row)
            else:
                print 'insert'
                ex1c.addUser(row)

def updateOrder(path):
    with open(path, "r") as file_obj:
        reader = csv.reader(file_obj, delimiter=ex1c.delimiter)
        for row in reader:
            if row[0]:
                print 'update'
                ex1c.updateOrder(row)
            else:
                print 'insert'
                ex1c.addOrder(row)

user_list = ex1c.users()
order_list = ex1c.orders()

#if __name__ == "__main__":
    #csv_writer(user_list, "files/output_user.csv")
    #csv_writer(order_list, "files/output_order.csv")
    #updateUser("files/1c_user.csv")
    #updateOrder("files/1c_order.csv")

dt = time.strftime("%Y-%m-%d %H:%M", time.localtime())
#ex1c.updateTimeExchange(dt)


# read files on directory 1c
files = os.listdir('files/1c')
fil = sorted(files)
for f in fil:
    print 'files/1c'+f
    try:
        updateUser("files/1c/" + f)
    except:
        continue

# Name file
file_name = time.strftime("%Y%m%d%H%M", time.localtime())
print file_name+'_order_site.csv'

ex1c.save()
ex1c.close()