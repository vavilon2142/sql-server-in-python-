import mysql.connector
import pandas as pd
import pyodbc

'''Здесь я использую созданную таблицу в MySQL для анализа данных. Я попросил покидать мне сложных задачек у друга который Data Analyst, вот что из этого получилось. Я подумал если прислать код на позицию data, то симбиоз SQL и Pyhon будет как-то неплохо смотреться.

База данных:MySQL

Тип подключения к базе: MySQL-connector-python

Схема базы:я предоставил файл с EER диаграмму для базы которая используется здесь "Restaurant7"
'''

cnx = mysql.connector.connect(auth_plugin='mysql_native_password', host='127.0.0.1', user= 'root',password='Qwe15002142', database='Restaurant7')
print(cnx)

#-- Задача Вывести имя шефа и количество приготовленных блюд во время раьоты Lucy Cooler
query = '''SELECT Staff2.Staff_name AS Chef_name, 
SUM(order_details.qnt_meal) AS Total_cooked
FROM Staff
JOIN order_details ON order_details.Date BETWEEN Staff.Date_start AND Staff.Date_end AND Staff.Staff_designation = ('Waiter') AND Staff.Staff_name = ('Lucy Cooler')
LEFT JOIN Menu ON order_details.meal_id = Menu.meal_id 
LEFT JOIN Staff AS Staff2 ON Menu.Shef_id = Staff2.Staff_id AND Staff2.Staff_designation = 'Chef' AND Staff2.IsCurrent = 'y' 
GROUP BY Menu.Shef_id;'''
df = pd.read_sql(sql=query, con=cnx)
print(df)


'''задача вывести: 
Названия хранимых продуктов, которые еще не были использованы в готовке после своего завоза.
Количество блюд дороже 1000, в состав которых входит этот продукт.
'''

query2 = '''SELECT Storage.Quantity, Storage.Amount_left, Storage.Item_name, Menu.Meal_name, Menu.price, CASE
WHEN Menu.Price > 1000 THEN 1
WHEN Menu.price <= 1000 THEN 0
ELSE ''
END AS COUNT
FROM Storage
JOIN Menu_recipe ON Storage.item_id = Menu_recipe.Item_id AND Storage.is_active ='y' AND Storage.Quantity = Storage.Amount_left 
JOIN Menu ON Menu_recipe.Meal_id = Menu.Meal_id AND Menu.is_active = 'y'
GROUP BY Storage.Item_id;'''
df1 = pd.read_sql(sql=query2, con=cnx)
print(df1)
print('\n')


'''Задача: Напиши запрос который выведет имена сотрудников, у которых хотя бы раз менялся номер телефона'''
query3 = '''SELECT *
FROM (SELECT Staff.*, LEAD (Staff.Phone) OVER(PARTITION BY Staff.Staff_id ORDER BY Staff.date_end) AS wf FROM Staff) AS Tabl
WHERE Tabl.phone != wf;'''
df2 = pd.read_sql(sql=query3, con=cnx)
print(df2)

query4 = '''WITH t  AS (
SELECT Staff.Staff_name FROM Staff
LEFT JOIN Staff AS Staff2 ON Staff.Staff_id = Staff2.Staff_id 
WHERE Staff.Phone != Staff2.Phone
GROUP BY Staff2.Staff_id
HAVING COUNT(Staff.Staff_id) > 1
)
SELECT * FROM t;
'''
df3 = pd.read_sql(sql=query4, con=cnx)
print(df3)
