#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import csv
import time
from function import Exchange1c

path_file = '/var/www/exchange_1c/files'

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

def updateRepair(path):
    with open(path, "r") as file_obj:
        reader = csv.reader(file_obj, delimiter=ex1c.delimiter)
        for row in reader:
            if row[0]:
                print 'update'
                ex1c.updateRepair(row)
            else:
                print 'insert'
                ex1c.addRepair(row)

user_list = ex1c.users()
order_list = ex1c.orders()
repair_list = ex1c.repairs()

dt_file = time.strftime("%y%m%d%H%M", time.localtime())

if user_list:
    csv_writer(user_list, path_file+"/site/user/site_user"+dt_file+".csv")

if order_list:
    csv_writer(order_list, path_file+"/site/order/site_order"+dt_file+".csv")

if repair_list:
    print 123
    csv_writer(repair_list, path_file+"/site/repair/site_repair"+dt_file+".csv")

file_user = os.listdir(path_file+"/1c/user/")
file_order = os.listdir(path_file + "/1c/order/")
file_repair = os.listdir(path_file + "/1c/repair/")

if len(file_user) > 0:
    users = sorted(file_user)
    for user in users:
        try:
            updateUser(path_file + "/1c/user/" + user)
            os.remove(path_file + "/1c/user/" + user)
        except:
            continue

if len(file_order) > 0:
    orders = sorted(file_order)
    for order in orders:
        try:
            updateOrder(path_file + "/1c/order/" + order)
            os.remove(path_file + "/1c/order/" + order)
        except:
            continue

if len(file_repair) > 0:
    repairs = sorted(file_repair)
    for repair in repairs:
        try:
            updateRepair(path_file + "/1c/repair/" + repair)
            os.remove(path_file + "/1c/repair/" + repair)
        except:
            continue

dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
ex1c.updateTimeExchange(dt)

ex1c.save()
ex1c.close()