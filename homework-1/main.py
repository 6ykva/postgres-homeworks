"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

with open('north_data/employees_data.csv') as file:
    employees_data = list(csv.reader(file))

with open('north_data/customers_data.csv') as file:
    customers_data = list(csv.reader(file))

with open('north_data/orders_data.csv') as file:
    orders_data = list(csv.reader(file))

conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="bykva",
    password="password"
)

with conn:
    with conn.cursor() as cur:

        cur.executemany('INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)', employees_data[1:])
        cur.execute("SELECT * FROM employees")

        cur.executemany('INSERT INTO customers VALUES (%s, %s, %s)', customers_data[1:])
        cur.execute("SELECT * FROM customers")

        cur.executemany('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)', orders_data[1:])
        cur.execute("SELECT * FROM orders")

        # запускаем все считывания
        rows = cur.fetchall()

conn.close()
