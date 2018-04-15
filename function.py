#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb

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

    def users(self):
        sql = "SELECT " \
              "u.id," \
              "p.1c_id," \
              "p.type_client_id," \
              "p.type_payment_id," \
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
              "p.created_at," \
              "p.updated_at," \
              "u.name," \
              "u.email " \
              "FROM exchanges AS e, users AS u " \
              "LEFT JOIN user_profiles as p ON p.user_id = u.id " \
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
              #"o.type_client_id," \
              #"o.client_name," \
              #"o.user_company," \
              #"o.phone," \
              "o.delivery_town," \
              "o.delivery_street," \
              "o.delivery_house," \
              "o.delivery_house_block," \
              "o.delivery_office," \
              "o.type_payment_id," \
              #"o.company_full," \
              #"o.edrpou," \
              #"o.inn," \
              #"code_index," \
              #"o.region," \
              #"o.area," \
              #"o.city," \
              #"o.street," \
              #"o.house," \
              #"o.house_block," \
              #"o.office," \
              "o.comment," \
              "o.status_id," \
              "o.created_at," \
              "o.updated_at " \
              "FROM exchanges AS e, orders AS o " \
              "WHERE o.updated_at > e.exchange"
        self.__session.execute(sql)
        orders = self.__session.fetchall()
        return orders

    def addUser(self, user):
        sql = "INSERT INTO users SET `name` = %s, email = %s, created_at = %s, updated_at = %s"
        self.__session.execute(sql, [user[25], user[26], user[23], user[24]])
        id = self.__session.lastrowid

        user[0] = id
        sql = "INSERT INTO user_profiles SET " \
              "user_id = %s," \
              "1c_id = %s," \
              "type_client_id = %s, " \
              "type_payment_id = %s," \
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
              "updated_at = %s" \

        self.__session.execute(sql, user[:25])
        #self.save()

    def updateUser(self, user):
        sql = "UPDATE users SET `name` = %s, email = %s, created_at = %s, updated_at = %s WHERE id=%s"
        self.__session.execute(sql, [user[25], user[26], user[23], user[24], user[0]])

        user[25] = user[0]

        sql = "UPDATE user_profiles SET " \
              "1c_id = %s," \
              "type_client_id = %s, " \
              "type_payment_id = %s," \
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
              "updated_at = %s " \
              "WHERE user_id = %s"

        self.__session.execute(sql, user[1:26])
        #self.save()

    def addOrder(self, order):
        sql = "INSERT INTO orders SET " \
              "user_id = %s," \
              "1c_id = %s," \
              "1cuser_id = %s," \
              "type_order_id = %s," \
              #"type_client_id = %s," \
              #"client_name = %s," \
              #"user_company = %s," \
              #"phone = %s," \
              "delivery_town = %s," \
              "delivery_street = %s," \
              "delivery_house = %s," \
              "delivery_house_block = %s," \
              "delivery_office = %s," \
              "type_payment_id = %s," \
              #"company_full = %s," \
              #"edrpou = %s," \
              #"inn = %s," \
              #"code_index = %s," \
              #"region = %s," \
              #"area = %s," \
              #"city = %s," \
              #"street = %s," \
              #"house = %s," \
              #"house_block = %s," \
              #"office = %s," \
              "comment = %s," \
              "status_id = %s," \
              "created_at = %s," \
              "updated_at = %s"

        self.__session.execute(sql, order[1:30])
        #self.save()

    def updateOrder(self, order):
        order.append(order[0])
        sql = "UPDATE orders SET " \
              "user_id = %s," \
              "1c_id = %s," \
              "1cuser_id = %s," \
              "type_order_id = %s," \
              #"type_client_id = %s," \
              #"client_name = %s," \
              #"user_company = %s," \
              #"phone = %s," \
              "delivery_town = %s," \
              "delivery_street = %s," \
              "delivery_house = %s," \
              "delivery_house_block = %s," \
              "delivery_office = %s," \
              "type_payment_id = %s," \
              #"company_full = %s," \
              #"edrpou = %s," \
              #"inn = %s," \
              #"code_index = %s," \
              #"region = %s," \
              #"area = %s," \
              #"city = %s," \
              #"street = %s," \
              #"house = %s," \
              #"house_block = %s," \
              #"office = %s," \
              "comment = %s," \
              "status_id = %s," \
              "created_at = %s," \
              "updated_at = %s " \
              "WHERE id = %s"

        self.__session.execute(sql, order[1:])
        #self.save()

    def updateTimeExchange(self, time_exchange):
        sql = "UPDATE exchanges SET exchange = %s WHERE id = 1"
        self.__session.execute(sql, [time_exchange])
