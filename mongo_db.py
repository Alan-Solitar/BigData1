from pprint import pprint
import pymongo
import pandas as pd

currDataFrame = pd.read_csv("./ROSMAP_RNASeq_entrez.csv")

client = pymongo.MongoClient()
db = client.ROSMAP_RNASeq_entrez
ROSMAP_Collection = db.ROSMAP_Collection


def insert_records():
    ROSMAP_Collection.insert_many(currDataFrame.to_dict('records'))

#insert_records()
pprint(ROSMAP_Collection.find_one({"PATIENT_ID" : "X164_120423"},{'DIAGNOSIS':1,'_id':0}))
