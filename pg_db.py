import psycopg2
import numpy as np
import pandas as pd
import pymongo
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from time import sleep

class PatientDataManager():

    def __init__(self,exists=False):
        self.base_file_name = 'patients.csv'
        self.base_file_name_diag = 'ROSMAP_RNASeq_entrez.csv'
        self.table_name = 'patients_info'
        self.table_name_diag = 'diagnosis_info'
        self.db_name = 'patients_db'
        self.user,self.pw = self.get_info()
        self.fields = self.parse_base_file(self.base_file_name)
        if not exists:
            self.initiate()
    def main_method(self):
        val = int(input('what do you want do?\n1.Load ROSMAP file\n2. load patient data file\n3.Query for patient information\n'))
        if val ==1:
            file_name = input('File Name?: ')
            self.populate_diagnosis_table(file_name)
        if val == 2:
            file_name = input('File Name?: ')
            self.populate_db(file_name)
        if val == 3:
            self.query_for_patient_info()
    def parse_base_file(self,file_name):
        df = pd.read_csv(file_name)
        return list(df.columns.values)
    def get_info(self):
        print('Entre postgres credentials:')
        return input('db_username: '),input('db_password: ')
    def create_db(self):
        conn_str = "dbname='postgres' user='{}' host='localhost' password='{}'".format(self.user,self.pw)
        conn = psycopg2.connect(conn_str)
        #This is needed to create the database
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        try:
            db_create = 'CREATE DATABASE {}'.format(self.db_name)
            cur.execute(db_create)
            conn.commit()
        except:
            'DB exists'
        cur.close()
        conn.close()
    def create_tables(self):
        conn = psycopg2.connect("dbname='{}' user='{}' host='localhost' password='{}'".format(self.db_name,self.user,self.pw))
        cur = conn.cursor()
        query1 = 'CREATE TABLE IF NOT EXISTS {} ({} TEXT PRIMARY KEY, {} TEXT, {} TEXT, {} TEXT)'.format(self.table_name,self.fields[0],self.fields[1],self.fields[2],self.fields[3])
        query2= 'CREATE TABLE IF NOT EXISTS {} ({} TEXT PRIMARY KEY, DIAGNOSIS TEXT)'.format(self.table_name_diag,self.fields[0])
        cur.execute(query1)
        cur.execute(query2)
        conn.commit()
        cur.close()
        conn.close()
    def populate_db(self,file_name):
        df = pd.read_csv(file_name)
        df.age = df.age.astype(str)
        tups = df.to_records(index=False)
        
        conn = psycopg2.connect("dbname='{}' user='postgres' host='localhost' password='pgpwd'".format(self.db_name))
        cur = conn.cursor()
        query = "INSERT INTO {} ({},{},{},{} ) VALUES(%s,%s,%s,%s)".format(self.table_name,self.fields[0],self.fields[1],self.fields[2],self.fields[3])
        cur.executemany(query, tups)
        conn.commit()
        cur.close()
        conn.close()
    def populate_diagnosis_table(self,file_name):
        df = pd.read_csv(file_name)
        df = df[['PATIENT_ID','DIAGNOSIS']]
        tups = df.to_records(index=False)
        
        conn = psycopg2.connect("dbname='{}' user='postgres' host='localhost' password='pgpwd'".format(self.db_name))
        cur = conn.cursor()
        query = "INSERT INTO {} ({},{}) VALUES(%s,%s)".format(self.table_name_diag,'PATIENT_ID','DIAGNOSIS')
        cur.executemany(query,tups)
        conn.commit()
        cur.close()
        conn.close()
    def get_patient_info(self,patient_id):
        conn = psycopg2.connect("dbname='{}' user='postgres' host='localhost' password='pgpwd'".format(self.db_name))
        cur = conn.cursor()
        query = "SELECT {}.*, {}.DIAGNOSIS FROM {} INNER JOIN {} ON {}.patient_id = {}.patient_id WHERE {}.{} = '{}'".format(self.table_name,self.table_name_diag,self.table_name,self.table_name_diag,self.table_name,self.table_name_diag,self.table_name,self.fields[0],patient_id)
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
        self.create_tables()
        self.populate_diagnosis_table(self.base_file_name_diag)
        self.populate_db(self.base_file_name)
    def query_for_patient_info(self):
        patient_id = input('patient_id: ')
        info = self.get_patient_info(patient_id)
        #diagnosis  = self.get_diagnosis(patient_id)
        if info != None:
            print (' '.join([val for val in info]))

#pdm = PatientDataManager(False)
#pdm.query_for_patient_info()