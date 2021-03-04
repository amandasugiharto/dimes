from pymongo import MongoClient
import pandas as pd
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
# make a new collection called 'hedge_fund', 'holdings', and 'changes'.
# 'hedge_fund' holds the newest change + holding
collection_hedge_fund = db['hedge_fund']
collection_holdings = db['holdings']
collection_changes = db['changes']


# functions to turn csv into a json file
def csv_to_json(filename, header=None):  # read csv into json format
    data = pd.read_csv(filename, header=0).iloc[:, 1:]  # skip the first column of csv
    return data.to_dict('records')


def changes_csv_to_json(filename, header=None):  # read csv into json format
    data = pd.read_csv(filename, header=0)  # don't need to skip the first column of csv
    return data.to_dict('records')


# insert holdings info into collections
for file in company_holdings:
    # Update/insert into database
    collection_hedge_fund.update_one({'_id': file.split('-')[0]},
                                     {'$set': {'newestholding': csv_to_json(os.path.join(company_root_path, file)),
                                               'date': (file.split('-')[1]).split('.')[0]}},
                                     upsert=True)
    collection_holdings.update_one({'_id': file.split('.')[0]},
                                   {'$set': {'_id': file.split('.')[0],
                                             'date': (file.split('-')[1]).split('.')[0],
                                             'holding': csv_to_json(os.path.join(company_root_path, file)),
                                             'company': file.split('-')[0]}},
                                   upsert=True)

# insert changes info into database
for file in company_changes:
    collection_hedge_fund.update_one({'_id': file.split('-')[0]},
                                     {'$set': {'newestchange': changes_csv_to_json(os.path.join(change_root_path, file)),
                                               'changedate': (file.split('-')[1]).split('.')[0]}},
                                     upsert=True)
    collection_changes.update_one({'_id': file.split('.')[0]},
                                  {'$set': {'_id': file.split('.')[0],
                                            'date': (file.split('-')[1]).split('.')[0],
                                            'change': changes_csv_to_json(os.path.join(change_root_path, file)),
                                            'company': file.split('-')[0]}},
                                  upsert=True)


# Find newest holding dictionary and turn it into pandas df, given a cik
def dict_to_df(cik):
    out = pd.DataFrame()
    data = collection_hedge_fund.find_one({'_id': cik}, {'_id': False, 'newestholding': True})
    for line in data.get('newestholding'):
        out = out.append([line], ignore_index=True)
    return out


# print(dict_to_df('850529'))

# Testing if collections setup properly
# for document in collection_changes.find({'company': "850529" } ):
#    print(document)

# for document in collection_holdings.find({'company': "850529" } ):
#    print(document)

# for document in collection_hedge_fund.find({'_id': "850529" } ):
#    print(document)

# find the 3 most recent holdings for that company
# last_10_holdings = collection_holdings.find({'company': '850529'}).sort([('date', -1)]).limit(3)
# for document in last_10_holdings:
#     print(document)

# Find most recent holding of a certain company and place into pandas dataframe. Format of {'Column name': 'Entry', 'Column name': 'Entry', ... }
# def find_newest():
# data = collection_hedge_fund.find_one({'_id': '850529'}, {'_id': False, 'newestholding': True})
# print(data)

#pd.DataFrame(