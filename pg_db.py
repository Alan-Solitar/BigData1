import psycopg2
import numpy as np
import pandas as pd
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from time import sleep

file1_name = 'patients.csv'
file2_name = 'ROSMAP_RNASeq_entrez.csv'
df = pd.read_csv(file1_name)
df.info()

db_name = 'patients_db'
table_name = 'patients_info'
fields = list(df.columns.values)

def get_info():
    return input('db_username: '),input('db_password: ')
def create_db():
    db_username,db_password = get_info()
    print(db_username,db_password)
    conn = psycopg2.connect("dbname='postgres' user='postgres' host='localhost' password='pgpwd'")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE
    cur = conn.cursor()
    db_create = 'CREATE DATABASE {}'.format(db_name)
    cur.execute(db_create)
    conn.commit()
    cur.close()
    conn.close()
def create_table():
    conn = psycopg2.connect("dbname='{}' user='postgres' host='localhost' password='pgpwd'".format(db_name))
    cur = conn.cursor()
    query = 'CREATE TABLE {} ({} TEXT PRIMARY KEY, {} TEXT, {} TEXT, {} TEXT)'.format(table_name,fields[0],fields[1],fields[2],fields[3])
    print(query)
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

def do_stuff():
    conn = psycopg2.connect("dbname='patients_db1' user='postgres' host='localhost' password='pgpwd'")
    cur = conn.cursor()
    cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    for table in cur.fetchall():
        print(table)
    cur.close()
    conn.close()

def populate_db(file_name):
    df = pd.read_csv(file_name)
    df.age = df.age.astype(str)
    df.info()
    #sleep(10000)
    tups = df.to_records(index=False)
    for i,t in enumerate(tups):
        tups[i] = (t[0],int(t[1]),t[2],t[3])
    
    conn = psycopg2.connect("dbname='{}' user='postgres' host='localhost' password='pgpwd'".format(db_name))
    cur = conn.cursor()
    query = "INSERT INTO {} ({},{},{},{} ) VALUES(%s,%s,%s,%s)".format(table_name,fields[0],fields[1],fields[2],fields[3])
    cur.executemany(query, tups)
    conn.commit()
    cur.close()
    conn.close()
def get_patient_info():
    patient_id = input('patient_id: ')
    conn = psycopg2.connect("dbname='{}' user='postgres' host='localhost' password='pgpwd'".format(db_name))
    cur = conn.cursor()
    query = "SELECT * FROM {} WHERE {} = '{}'".format(table_name,fields[0],patient_id)
    cur.execute(query)
    print(cur.fetchone())
    cur.close()
    conn.close()
def initiate():
    create_db()
    sleep(10)
    create_table()


#initiate()
#populate_db(file1_name)
get_patient_info()
