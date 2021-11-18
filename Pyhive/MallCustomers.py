"""
@Author: Samarth BM
@Date: 2021-11-17
@Last Modified by: Samarth BM
@Title : Program to insert a .csv file from hdfs into hive database using pyhive library,and perform different query.
"""

from pyhive import hive
from LogHandler import logger
import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv(".env")


host_name = os.getenv("HOST_NAME")
port = os.getenv("PORT")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
database = os.getenv("DATABASE")


def createConnection():

    """
    Description:
        This method is used to create a connection with hive.
    """
   
    conn = hive.Connection(host=host_name, port=port, username=user, password=password,
                           database=database,  auth='CUSTOM')

    return conn

def create_database(db_name):

    """
    Description:
        This method is used to create a hive database.
    Parameter:
        It takes database name as a parameter for creating database.
    """
   
    try:
        connection = hive.Connection(host=host_name, port=port, username=user, password=password,
                                     auth='CUSTOM')

        cur = connection.cursor()
        cur.execute("CREATE DATABASE {}".format(db_name))
        logger.info("Database created successfully")

    except Exception as e:
        logger.error(e)


def create_table():

    """
    Description:
        This method is used to create a table in a hive database.
    """
    
    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("create table customerdetails(Id int, Gender string, Age int, Income int, Expenditure int)row format delimited fields terminated by ',' stored as textfile location 'hdfs://localhost:9000/MallCustomers' tblproperties('skip.header.line.count'='1')")
        logger.info("Table has been created successfully")

    except Exception as e:
        logger.error(e)

def where_clause():

    """
    Description:
        This method is used to perform where clause query.
    """

    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("select * from customerdetails where gender = 'Male'")
        print(cur.fetchall())

    except Exception as e:
        logger.error(e)

def sort():
    """
    Description:
        This method is used to perform sort query.
    """
    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("select * from customerdetails sort by age")
        print("After sorting")
        print(cur.fetchall())

    except Exception as e:
        logger.error(e)

def create_dataframe():

    """
    Description:
        This method is used to create a panda dataframe by doing query with a hive database.
    """
    
    try:
        conn = createConnection()
        df = pd.read_sql("select * from customerdetails sort by age",conn)
        print("Sorting using dataframe")
        print(df)

    except Exception as e:
        logger.error(e)

def drop(tbl_name):

    """
    Description:
        This method is used to delete a hive database table.
    """
   
    try:
        connection = createConnection()
        cur = connection.cursor()
        cur.execute("drop table {}".format(tbl_name))
        logger.info("Table Deleted Successfully")

    except Exception as e:
        logger.error(e)


create_database("mall_customers")
create_table()
where_clause()
sort()
create_dataframe()
drop("customerdetails")