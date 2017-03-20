import psycopg2
import numpy as np
import pandas as pd
import pymongo
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from time import sleep

class PatientDataManager():

    def __init__(self,exists=False):
        self.base_file_name = 'patients.csv'
        self.table_name = 'patients_info'
        self.db_name = 'patients_db'
        self.user,self.pw = self.get_info()
        self.fields = self.parse_base_file(self.base_file_name)
        if not exists:
            self.initiate()
    def parse_base_file(self,file_name):
        df = pd.read_csv(file_name)
        return list(df.columns.values)
    def get_info(self):
        return input('db_username: '),input('db_password: ')
    def create_db(self):
        conn_str = "dbname='postgres' user='{}' host='localhost' password='{}'".format(self.user,self.pw)
        conn = psycopg2.connect(conn_str)
        #This is needed to create the database
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        db_create = 'CREATE DATABASE {}'.format(self.db_name)
        cur.execute(db_create)
        conn.commit()
        cur.close()
        conn.close()
    def create_table(self):
        conn = psycopg2.connect("dbname='{}' user='{}' host='localhost' password='{}'".format(self.db_name,self.user,self.pw))
        cur = conn.cursor()
        query = 'CREATE TABLE {} ({} TEXT PRIMARY KEY, {} TEXT, {} TEXT, {} TEXT)'.format(self.table_name,self.fields[0],self.fields[1],self.fields[2],self.fields[3])
        print(query)
        cur.execute(query)
        conn.commit()
        cur.close()
        conn.close()

    def populate_db(self,file_name):
        df = pd.read_csv(file_name)
        df.age = df.age.astype(str)
        df.info()
        tups = df.to_records(index=False)
        for i,t in enumerate(tups):
            tups[i] = (t[0],int(t[1]),t[2],t[3])
        conn = psycopg2.connect("dbname='{}' user='postgres' host='localhost' password='pgpwd'".format(self.db_name))
        cur = conn.cursor()
        query = "INSERT INTO {} ({},{},{},{} ) VALUES(%s,%s,%s,%s)".format(self.table_name,self.fields[0],self.fields[1],self.fields[2],self.fields[3])
        cur.executemany(query, tups)
        conn.commit()
        cur.close()
        conn.close()
    def get_patient_info(self,patient_id):
        conn = psycopg2.connect("dbname='{}' user='postgres' host='localhost' password='pgpwd'".format(self.db_name))
        cur = conn.cursor()
        query = "SELECT * FROM {} WHERE {} = '{}'".format(self.table_name,self.fields[0],patient_id)
        cur.execute(query)
        info = (cur.fetchone())
        cur.close()
        conn.close()
        return info
    def get_diagnosis(self,patient_id):
        client = pymongo.MongoClient()
        db = client.ROSMAP_RNASeq_entrez
        ROSMAP_Collection = db.ROSMAP_Collection
        return ROSMAP_Collection.find_one({"PATIENT_ID" : patient_id},{'DIAGNOSIS':1,'_id':0})['DIAGNOSIS']
    def initiate(self):
        self.create_db()
        self.create_table()
        self.populate_db(self.base_file_name)
    def query_for_patient_info(self):
        patient_id = input('patient_id: ')
        info = self.get_patient_info(patient_id)
        diagnosis  = self.get_diagnosis(patient_id)
        print (' '.join([val for val in info]),diagnosis)

pdm = PatientDataManager(True)
pdm.query_for_patient_info()