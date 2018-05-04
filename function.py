#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import time

class Exchange1c:
    __host = '127.0.0.1'
    __user = 'root'
    __password = '170270'
    __database = 'sint_odessa'
    __session = None
    __connection = None
    delimiter = ';'

    def open(self):
        try:
            con = MySQLdb.connect(self.__host, self.__user, self.__password, self.__database, charset='utf8', use_unicode=False)
            self.__connection = con
            self.__session = con.cursor()
        except MySQLdb.Error as e:
            print "Error %d: %s" % (e.args[0], e.args[1])

    def save(self):
        self.__connection.commit()

    def close(self):
        self.__session.close()
        self.__connection.close()

    def current_time(self):
        dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        return dt

    def users(self):
        sql = "SELECT " \
              "u.id," \
              "p.1c_id," \
              "p.type_client_id," \
              "p.type_payment_id," \
              "p.office_id," \
              "p.client_name," \
              "p.delivery_town," \
              "p.delivery_street," \
              "p.delivery_house," \
              "p.delivery_house_block," \
              "p.delivery_office," \
              "p.phone," \
              "p.user_company," \
              "p.company_full," \
              "p.edrpou," \
              "p.inn," \
              "p.code_index," \
              "p.region," \
              "p.area," \
              "p.city," \
              "p.street," \
              "p.house," \
              "p.house_block," \
              "p.office," \
              "u.name," \
              "u.email " \
              "FROM exchanges AS e, users AS u " \
              "INNER JOIN user_profiles as p ON p.user_id = u.id " \
              "WHERE u.updated_at > e.exchange OR p.updated_at > e.exchange"
        self.__session.execute(sql)
        users = self.__session.fetchall()
        return users

    def orders(self):
        sql = "SELECT " \
              "o.id," \
              "o.user_id," \
              "o.1c_id," \
              "o.1cuser_id," \
              "o.type_order_id," \
              "o.type_payment_id," \
              "o.office_id," \
              "o.delivery_town," \
              "o.delivery_street," \
              "o.delivery_house," \
              "o.delivery_house_block," \
              "o.delivery_office," \
              "o.comment," \
              "o.status_id " \
              "FROM exchanges AS e, orders AS o " \
              "WHERE o.updated_at > e.exchange"
        self.__session.execute(sql)
        orders = self.__session.fetchall()
        return orders

    def repairs(self):
        sql = "SELECT " \
              "r.id, " \
              "r.order_id," \
              "r.status_repair_id," \
              "r.device," \
              "r.set_device," \
              "r.text_defect," \
              "r.diagnostic," \
              "r.cost," \
              "r.comment," \
              "r.user_consent_id " \
              "FROM exchanges AS e, act_repairs AS r " \
              "WHERE r.updated_at > e.exchange"
        self.__session.execute(sql)
        repairs = self.__session.fetchall()
        return repairs

    def addUser(self, user):
        dt = self.current_time()
        sql = "INSERT INTO users SET `name` = %s, email = %s, created_at = %s, updated_at = %s"
        self.__session.execute(sql, [user[24], user[25], dt, dt])
        id = self.__session.lastrowid
        user[0] = id
        user[24] = dt
        user[25] = dt
        sql = "INSERT INTO user_profiles SET " \
              "user_id = %s," \
              "1c_id = %s," \
              "type_client_id = %s, " \
              "type_payment_id = %s," \
              "office_id = %s," \
              "client_name = %s," \
              "delivery_town = %s," \
              "delivery_street = %s," \
              "delivery_house = %s," \
              "delivery_house_block = %s," \
              "delivery_office = %s," \
              "phone = %s," \
              "user_company = %s," \
              "company_full = %s," \
              "edrpou = %s," \
              "inn = %s," \
              "code_index = %s," \
              "region = %s," \
              "area = %s," \
              "city = %s," \
              "street = %s," \
              "house = %s," \
              "house_block = %s," \
              "office = %s," \
              "created_at = %s," \
              "updated_at = %s"
        self.__session.execute(sql, user)

    def updateUser(self, user):
        dt = self.current_time()
        sql = "UPDATE users SET `name` = %s, email = %s, updated_at = %s WHERE id=%s"
        self.__session.execute(sql, [user[24], user[25], dt, user[0]])
        user[24] = dt
        user[25] = user[0]
        sql = "UPDATE user_profiles SET " \
              "1c_id = %s," \
              "type_client_id = %s, " \
              "type_payment_id = %s," \
              "office_id = %s," \
              "client_name = %s," \
              "delivery_town = %s," \
              "delivery_street = %s," \
              "delivery_house = %s," \
              "delivery_house_block = %s," \
              "delivery_office = %s," \
              "phone = %s," \
              "user_company = %s," \
              "company_full = %s," \
              "edrpou = %s," \
              "inn = %s," \
              "code_index = %s," \
              "region = %s," \
              "area = %s," \
              "city = %s," \
              "street = %s," \
              "house = %s," \
              "house_block = %s," \
              "office = %s," \
              "updated_at = %s " \
              "WHERE user_id = %s"
        self.__session.execute(sql, user[1:])

    def addOrder(self, order):
        dt = self.current_time()
        order.append(dt)
        order.append(dt)
        sql = "INSERT INTO orders SET " \
              "user_id = %s," \
              "1c_id = %s," \
              "1cuser_id = %s," \
              "type_order_id = %s," \
              "type_payment_id = %s," \
              "office_id = %s," \
              "delivery_town = %s," \
              "delivery_street = %s," \
              "delivery_house = %s," \
              "delivery_house_block = %s," \
              "delivery_office = %s," \
              "comment = %s," \
              "status_id = %s," \
              "created_at = %s," \
              "updated_at = %s"
        self.__session.execute(sql, order[1:])

    def updateOrder(self, order):
        dt = self.current_time()
        order.append(dt)
        order.append(order[0])
        sql = "UPDATE orders SET " \
              "user_id = %s," \
              "1c_id = %s," \
              "1cuser_id = %s," \
              "type_order_id = %s," \
              "type_payment_id = %s," \
              "office_id = %s," \
              "delivery_town = %s," \
              "delivery_street = %s," \
              "delivery_house = %s," \
              "delivery_house_block = %s," \
              "delivery_office = %s," \
              "comment = %s," \
              "status_id = %s," \
              "updated_at = %s " \
              "WHERE id = %s"
        self.__session.execute(sql, order[1:])

    def addRepair(self, repair):
        dt = self.current_time()
        repair.append(dt)
        repair.append(dt)
        sql = "INSERT INTO act_repairs SET " \
              "order_id = %s," \
              "status_repair_id = %s," \
              "device = %s," \
              "set_device = %s," \
              "text_defect = %s," \
              "diagnostic = %s," \
              "cost = %s," \
              "comment = %s," \
              "user_consent_id = %s," \
              "created_at = %s," \
              "updated_at = %s"
        self.__session.execute(sql, repair[1:])

    def updateRepair(self, repair):
        dt = self.current_time()
        repair.append(dt)
        repair.append(repair[0])
        sql = "UPDATE act_repairs SET " \
              "order_id = %s," \
              "status_repair_id = %s," \
              "device = %s," \
              "set_device = %s," \
              "text_defect = %s," \
              "diagnostic = %s," \
              "cost = %s," \
              "comment = %s," \
              "user_consent_id = %s," \
              "updated_at = %s " \
              "WHERE id = %s"
        self.__session.execute(sql, repair[1:])

    def updateTimeExchange(self, time_exchange):
        sql = "UPDATE exchanges SET exchange = %s WHERE id = 1"
        self.__session.execute(sql, [time_exchange])
