from pymongo import MongoClient, TEXT
import pandas as pd
import json
# import os

# path = "C://Users//Tim//PycharmProjects//Stat359FormParsing//850529-20130716.csv"
# os.chdir(path)

# myclient = pymongo.MongoClient("mongodb://localhost:27017/")

# setup database
client = MongoClient('localhost', 27017)
database_list = client.list_database_names()
if "13F" in database_list:
    client.drop_database("13F")
db = client['13F']
# make a new collection called 'hedge_fund'
collection_hedge_fund = db['hedge_fund']


# function to turn csv into a json file
def csv_to_json(filename, header=None):  # read csv into json format
    data = pd.read_csv(filename, header=0).iloc[:, 1:]  # skip the first column of csv
    return data.to_dict('records')


# convert json into a string to make a new dictionary with key: CIK and value: 13F data
my_json_string = json.dumps({'850529': csv_to_json('850529-20130716.csv')})
# converting string to json again to add a document to the collection
final_dictionary = json.loads(my_json_string)

# insert document into collection
collection_hedge_fund.insert_one(final_dictionary)

# print(csv_to_json('850529-20130716.csv'))
# collection_hedge_fund.insert_many(csv_to_json('850529-20130716.csv'))
# print(db.list_collection_names())

for document in db.hedge_fund.find(): # print out documents in collection
    print(document)