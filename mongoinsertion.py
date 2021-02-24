from pymongo import MongoClient, TEXT
import pandas as pd
import json
import os
import functools

company_root_path = "/Users/Tim/PycharmProjects/Stat359Form13FParsing/per_company_date"
change_root_path = "/Users/Tim/PycharmProjects/Stat359Form13FParsing/change_dfs"


# wrote sorter function to sort out the files since sorted() didn't work properly
# Note: I deleted the .DS file before running this code since on Windows
def file_sorter(filename1, filename2):
    companyid1 = int(filename1.split('-')[0])
    suffix1 = int(filename1.split('-')[1][0:-4])
    companyid2 = int(filename2.split('-')[0])
    suffix2 = int(filename2.split('-')[1][0:-4])
    # first sort by company ID, then by the date or number after the dash
    if companyid1 != companyid2:
        return companyid1 - companyid2
    return suffix1 - suffix2


company_holdings = sorted(os.listdir(company_root_path),
                          key=functools.cmp_to_key(file_sorter))
company_changes = sorted(os.listdir(change_root_path),
                         key=functools.cmp_to_key(file_sorter))

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


def changes_csv_to_json(filename, header=None):  # read csv into json format
    data = pd.read_csv(filename, header=0)  # don't need to skip the first column of csv
    return data.to_dict('records')


# insert documents into collection
for file in company_holdings:
    # check if the document does not exist yet. If so, make a new one
    if collection_hedge_fund.count_documents({'_id': file.split('-')[0]}, limit=1) == 0:
        # insert document into collection if document does not exist
        collection_hedge_fund.insert_one({'_id': file.split('-')[0],
                                          'holdings': {(file.split('-')[1]).split('.')[0]:
                                                           csv_to_json(os.path.join(company_root_path, file))}})
    else:
        # Otherwise update field in document
        collection_hedge_fund.update_one({'_id': file.split('-')[0]},
                                         {'$set': {'holdings': {(file.split('-')[1]).split('.')[0]:
                                                           csv_to_json(os.path.join(company_root_path, file))}}})

#insert changes into database
for file in company_changes:
    # check if the changes field does not exist yet. If so, make a new one
    # if collection_hedge_fund.count_documents({'_id': file.split('-')[0],
    #                                           'changes': 0}, limit=1) == 0:
        collection_hedge_fund.update_one({'_id': file.split('-')[0]},
                                          {'$set': {'changes': {(file.split('-')[1]).split('.')[0]:
                                                                     changes_csv_to_json(
                                                                         os.path.join(change_root_path, file))}}},
                                         upsert=True)
    # else:
    #     # Otherwise update field in document
    #     collection_hedge_fund.update_one({'_id': file.split('-')[0]},
    #                                      {'$set': {'changes': {(file.split('-')[1]).split('.')[0]:
    #                                                                 changes_csv_to_json(
    #                                                                     os.path.join(change_root_path, file))}}})

# print out documents in collection
#for document in db.hedge_fund.find({} , { 'holdings': 0} ):
#    print(document)

# print out documents in collection matching specific CUSIP
#for document in db.hedge_fund.find({'_id': "850529" } , { 'holdings': 0} ):
#    print(document)

# print out how many distinct id's
# print(len(db.hedge_fund.distinct('_id')))
#print(len(db.hedge_fund.distinct('changes')))
#print(db.hedge_fund.find_one_and_update({'_id': "850529", 'change': 0}, {'$inc': {'count': 1}, '$set': {'done': True}}))


