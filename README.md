<p align="center">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/Title2.jpg?raw=true"/>
</p>

<h6 align="center">
Authors: Tim Liao, Amanda Sugiharto, and Austen Wahl
</h6>

## Overview
The purpose of this project was to create a database of stock holdings of top hedge funds that contained quarterly changes and had the ability to be updated periodically.

## Data Source
[SEC Edgar 13-F Filings](https://www.sec.gov/edgar/searchedgar/companysearch.html)

## Tools
- Python
- Python-edgar package
- Beautiful Soup package
- MongoDB

## Project Stages
1. Collect and Parse Data
2. Calculate the Change in Holdings between Quarters
3. Integrate Data from Steps 1 and 2 into MongoDB
4. Periodical Updates
&ensp;

<h1 align="center">
Step 1: Collect and Parse Data
</h1>

### Data Source

All of our data for this project is from the SEC website, and is downloaded using the python-edgar package. 

### Getting 13-F's

Using the command lines below, subbing in your own pathways, you can download python-edgar and index filings.

```
pip install python-edgar
python run.py -y 1993
type *.tsv > master.tsv
find "CIK" C:\Users\...\master.tsv > CIK.txt
find "Company Name" C:\Users\...\CIK.tsv > Cik2.txt
type CIK.txt | find "13F-HR" > 000CIK.txt (or 0000CIK.txt if six digit CIK)
```

After you have all the indexed filings, ensure your path is set correctly and initialize an empty dataframe. After that, you can load in 13F's into the dataframes.

```python
root_path = "/Users/..."
company_files = os.listdir(root_path)

all_hedgefunds = pd.DataFrame()

for file in company_files:
    path = os.path.join("13F", file)
    hedgefund_item = pd.read_csv(path, sep="|", index_col=False,
                  names=["cik", "name", "form_type", "date", "txt", "html"])
    all_hedgefunds = all_hedgefunds.append(hedgefund_item)
```

Next, clean the html and date columns of the dataframes and subset the data to 2013 or later (process does not work on forms before 2013). Get the html pages that have the "information table" format. 

```python
all_hedgefunds["html"] = "https://www.sec.gov/Archives/" + all_hedgefunds["html"]
all_hedgefunds["date"] = pd.to_datetime(all_hedgefunds["date"])

all_hedgefunds_new2 = all_hedgefunds[all_hedgefunds["date"] >= "2012-01-01"]

is_new2 = []
for link in all_hedgefunds_new2["html"]:
    html = requests.get(link).text
    soup = bs4.BeautifulSoup(html, 'lxml')
    # rows = soup.find_all('tr')[1:]
    if soup.find("td", text="INFORMATION TABLE") != None:
        is_new2.append(True)
    else:
        is_new2.append(False)
    # Don't go over SEC's traffic quota of 10 requests per second
    time.sleep(0.11)
```

This part of the process is accomplished using the Beautiful Soup package. The SEC also has a traffic quota, so it is important to ensure that you do not go over that quota.
Next, you'll want to append an indicator to show whether or not there is a new format in the received filing.

```python
all_hedgefunds_new2.loc[:,"has_info_table"] = is_new2
all_hedgefunds_new2_infotable = all_hedgefunds_new2[all_hedgefunds_new2["has_info_table"] == True]

# Get the link to the actual information table from the html webpage
infotable_links = []
for link in all_hedgefunds_new2_infotable["html"]:
    html = requests.get(link).text
    soup = bs4.BeautifulSoup(html, 'lxml')
    rows = soup.find_all('tr')[1:]
    try:
        infotable = soup.find("td", text="INFORMATION TABLE").find_previous_sibling("td").a.get("href")
        infotable_links.append(infotable)
    except:
        infotable_links.append(None)
    time.sleep(0.11)
    
all_hedgefunds_new2_infotable.loc[:,"infotable_link"] = infotable_links
all_hedgefunds_new2_infotable.loc[:,"infotable_link"] = "https://www.sec.gov" + all_hedgefunds_new2_infotable["infotable_link"]
```
After that, you want to get the link to the actual information table from the html webpage. This can be done using Beautiful Soup as well, and an example of where this process is taking you on the SEC website is listed below.

<p align="center">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/1b.jpg?raw=true"/>
</p>

You have almost completed parsing and downloading all the relvant 13-F's, now you just need to check how many extractions failed, reindex the dataframe with a sequential index, and save the files per company per filing in the format "CIK-Date.csv." All of the code to accomplish these steps is listed below:

```python
all_hedgefunds_new2_infotable["infotable_link"].isna().sum()

holdings_df = []
for info_link in all_hedgefunds_new2_infotable["infotable_link"]:
    try:
        df = pd.read_html(info_link, header=2)[-1]
        holdings_df.append(df)
    except:
        holdings_df.append("Unable to get holdings")
    time.sleep(0.1)

all_hedgefunds_new2_infotable.index = range(0,len(holdings_df))

for i in range(0,len(holdings_df)):
    holdings = holdings_df[i]
    cik = all_hedgefunds_new2_infotable["cik"][i]
    date = all_hedgefunds_new2_infotable["date"][i].strftime("%Y%m%d")
    file_name = "per_company_date/" + str(cik) + "-" + str(date) + ".csv"
    try:
        holdings.to_csv(file_name)
```

Now you have saved the 13-F forms for each hedge fund in .csv form with all the relevant information (holdings, etc.).

### Note

*This process will only work with 13F forms that are all in the same, current format, so forms before 2013 will not be able to be parsed and put into a dataframe with this code.*

We, personally, were not too worried about including data from before 2013 as we wanted to use this database to evaluate the near past, present, and future, but if you'd like to include that data you will have to use a different process to parse.

### Example

Below is an example of what the parsed dataframe of the quarterly 13F of a single hedge fund looks like. (The numbers on top represent the CIK of the individual hedge fund and the date of the 13F)

<p align="center">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/ParsedDataFrame.jpg?raw=true"/>
</p>

[Link to Code for Framing Parsed Data](https://github.com/amandasugiharto/stat359/blob/main/13f_dataframe.py)

<h1 align="center">
Step 2: Calculate Change in Holdings between Quarters
</h1>

### Intro
Once we had all of our parsed dataframes, we decided to calculate the total changes in holdings between quarters for all of the hedge funds in our database. If you would like to calculate other metrics from filing to filing, you can use a similar process to the one that follows:

### Code for creating change dataframes
Specify your root path and ensure that you have installed the pandas, os, and itertools packages.

<p align="left">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/2-1.jpg?raw=true"/>
</p>

In order to create a dataframe that contains quarterly changes from new/old 13F filings, you must first load in the parsed dataframes from Step 1.
Next, get the lists of holdings in previous time periods and the current time period. Reindex the dataframes using CUSIP and that will have cleaned your data for further analysis.

Often, hedge funds will acquire a new stock over a quarterly period, or they will sell out of one. It is important to determine what the new holdings are, and that can be accomplished with the code listed above.

<p align="left">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/2-2.jpg?raw=true"/>
</p>

Similarly to finding new holdings, you must also determine if any holdings were sold out in the previous quarter. The code to accomplish that is bracketed and listed above.

After you have determined which holdings are new and sold out, you can create 3 new dataframes consisting of all 3 scenarios (New, Not New, Sold Out).
Then, you can initialize a larger dataframe containing information of all the holdings. With this information it is now possible to calculate the changes in holdings from quarter to quarter. By creating a subset of holdings that were in both the new and old filings, you can write an equation to calculate change metrics. 

<p align="left">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/2-3.jpg?raw=true"/>
</p>

Finally, to complete this step of the process you should append the status of each holding to the large dataframe, and join the change, new, and sold out dataframes to the main one.

Now, you should have a dataframe that resembles the one below, which contains quarterly change information, along with whether the stock is a new holding, not a new holding, or has been sold out.

<p align="left">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/2-4.jpg?raw=true"/>
</p>

Lastly, you will likely want to complete the above process for all the available 13F's of all the hedge funds in your database. The code to accomplish that process is below:

```python
cik_list = []
for company in company_holdings[1:]:
    separated = company.split("-")
    cik = separated[0]
    cik_list.append(cik)
cik_list_unique = list(set(cik_list))

for cik in cik_list_unique:
    filing_list = [filing for filing in company_holdings if filing.startswith(cik)]
    filing_list_length = len(filing_list)
    for i in range(0,filing_list_length-1):
        df = calculate_secondary(filing_list[i], filing_list[i+1])
        new_date = filing_list[i+1].split("-")[1].split(".")[0]
        df.to_csv("PathforChangeFilings" + cik + "-" + str(new_date) + ".csv")
```
[Link to Code for Calculating Change](https://github.com/amandasugiharto/stat359/blob/main/calculate_change.py)

<h1 align="center">
Step 3: Integrate Data into MongoDB
</h1>

### MongoDB

MongoDB is very useful because it creates collections that hold tons of data, and these collections do not require documents to have the same schema. However, in order to make use of MongoDB files must be converted into a json format.

### Process

The first thing you should do when beginning step 3 is to ensure your packages and root paths are correct.

```python
from pymongo import MongoClient
import pandas as pd
import os
import functools

company_root_path = "folder with 13F data"
change_root_path = "folder with change data"
```
Next, begin creating your database. The following Python code shows how to create a database in MongoDB. We decided to create three collections: "Hedge Fund," "Holdings," and "Changes."

```python
client = MongoClient('localhost', 27017)
database_list = client.list_database_names()
if "13F" in database_list:
    client.drop_database("13F")
db = client['13F']

collection_hedge_fund = db['hedge_fund']
collection_holdings = db['holdings']
collection_changes = db['changes']
```
Once you have decided what to name your database and how to allocate the data, change the format of all the files you previously had from csv to json. Doing this ensures that MongoDB has data in the format it prefers, and makes MongoDB able to easily access the information and increases its utility as a database holder.

```python
def csv_to_json(filename, header=None):  # read csv into json format
    data = pd.read_csv(filename, header=0).iloc[:, 1:]  # skip the first column of csv
    return data.to_dict('records')


def changes_csv_to_json(filename, header=None):  # read csv into json format
    data = pd.read_csv(filename, header=0)  # don't need to skip the first column of csv
    return data.to_dict('records')
```
After the files are in json format, you can insert all the holdings info into whatever collection you'd like. Our process is below:

```python
for file in company_holdings:
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
```
Congratulations, your data is now in MongoDB in an easily accessible format. 

Using the above code, if the document exists, it is updated in the database, and if it does not exist then it is inserted into the database.
In order to find the newest holding dictionary and turn it into pandas df, given a cik, use the following code:

```python
def dict_to_df(cik):
    out = pd.DataFrame()
    data = collection_hedge_fund.find_one({'_id': cik}, {'_id': False, 'newestholding': True})
    for line in data.get('newestholding'):
        out = out.append([line], ignore_index=True)
    return out
```

### Example of MongoDB Database

<p align="center">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/3.jpg?raw=true"/>
</p>

An image of what our database within MongoDB looks like is above. Again, this part of the process is very customizable, and you can format/create your database however you want within MongoDB once you have converted your files into a json format.

[Link to Code for Creating MongoDB Database](https://github.com/amandasugiharto/stat359/blob/main/mongoinsertion.py)

<h1 align="center">
Step 4: Periodical Updates
</h1>

There are five main parts of periodically updating the database within MongoDB.

<p align="left">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/4.jpg?raw=true"/>
</p>

### Note

Occassionally, a hedge fund will change its' name or will go out of business entirely. Make sure to check for these sorts of things, as it could create errors in the formatting of your database.

*Many of these parts are reusing code from earlier steps in the process.*

### Parts 1/2: Get Most Recent Filings from SEC & Sort 13-F Files

The code for doing parts one and two of the database update is listed below. The "def main" commands are Windows specific; you do not need to write this out if on Apple computer.

```python
# import json
import os

# this def main stuff is Windows specific, downloading the newest index file for Q1 2021
def main():
    import edgar
    edgar.download_index(os.path.join(os.getcwd(),"index_files_new"), 2021)

if __name__ == '__main__':
    main()

# function to find the correct companies
def find_companies(company_list):
    for company in company_list:
        infile = open(os.path.join(os.getcwd(), "index_files_new/2021-QTR1.tsv"),
                      'r')  # This line is referencing the new index file
        outfile = open(os.path.join(os.getcwd(),"index_files_new/companies/") + company[1] + '.txt',
                       'w')
        for line in infile:
            if line.find(company[0]) != -1:
                outfile.write(line)
            else:
                continue
        infile.close()
        outfile.close()

# function to find the correct forms
def find_forms(form_string, encoding = "utf-8"):
    directory = os.path.join(os.getcwd(), "index_files_new/companies")
    for filename in os.listdir(directory):
        infile = open(os.path.join(directory, filename), 'r', encoding=encoding)
        outfile = open(os.path.join(os.getcwd(), "index_files_new/") + form_string + '/' + filename,
                       'w')
        for line in infile:
            if line.find(form_string) != -1:
                outfile.write(line)
            else:
                continue
        infile.close()
        outfile.close()

# write out new files
find_companies(data)
find_forms('13F', encoding="mac_roman")

# Specify path with new 13F files per company and get list of files
root_path = os.path.join(os.getcwd(), "index_files_new/13F")
company_files = os.listdir(root_path)
company_files.remove(".DS_Store")
```
You are also able to complete Step 1 of this process by using the command line from Step 1 and changing the year from 1993 to whatever the current year is. That will download all the filings on the SEC website since the beginning of the current year.

### Part 3: Parse and Clean New Filings

After you have your list of filings, you must now clean the filings and read the data into a dataframe. Similarly to in Step 1 of the project, you must also clean the columns within those dataframes and get the links for the newest filings. After reindexing and appending the dataframes that you have created, you can save the new cleaned files which can be used in calculating changes and inserted into MongoDB.

```python
# define function to clean new filings and save parsed new filings per company per date
def clean_new_filings(company_files, folder_path):
    import pandas as pd
    # Initialize empty dataframe
    all_hedgefunds = pd.DataFrame()

    # Read in the data into a dataframe
    for file in company_files:
        path = os.path.join(root_path, file)
        hedgefund_item = pd.read_csv(path, sep="|", index_col=False,
                      names=["cik", "name", "form_type", "date", "txt", "html"])
        all_hedgefunds = all_hedgefunds.append(hedgefund_item)

    # Clean the html and date columns
    all_hedgefunds["html"] = "https://www.sec.gov/Archives/" + all_hedgefunds["html"]
    all_hedgefunds["date"] = pd.to_datetime(all_hedgefunds["date"])

    import requests
    import bs4
    import time

    # Get infotable links for new filings
    infotable_links = []
    for link in all_hedgefunds["html"]:
        html = requests.get(link).text
        soup = bs4.BeautifulSoup(html, 'lxml')
        try:
            infotable = soup.find("td", text="INFORMATION TABLE").find_previous_sibling("td").a.get("href")
            infotable_links.append(infotable)
        except AttributeError:
            try:
                infotable = soup.find("td", text="INFORMATION TABLE").find_next_sibling("td").a.get("href")
                infotable_links.append(infotable)
            except:
                infotable_links.append(None)
        time.sleep(0.11)

    # Add infotable links to dataframe
    all_hedgefunds.loc[:,"infotable_link"] = infotable_links
    all_hedgefunds.loc[:,"infotable_link"] = "https://www.sec.gov" + all_hedgefunds["infotable_link"]

    # Reindex the dataframe with sequential index
    all_hedgefunds.index = range(0,len(all_hedgefunds.index))

    # get the ammendments dataframes and the dataframes to ammend
    ammendments = all_hedgefunds[all_hedgefunds["form_type"] == "13F-HR/A"]
    to_ammend = all_hedgefunds[(all_hedgefunds["form_type"] == "13F-HR") & (all_hedgefunds["cik"].isin(ammendments["cik"].tolist()))]

    # update the to_ammend dataframes with the ammendments
    ammended_holdings = []
    for cik in to_ammend["cik"]:
        holding_to_ammend_link = to_ammend[to_ammend["cik"] == cik].get("infotable_link")
        holding_to_ammend = pd.DataFrame()
        for link in holding_to_ammend_link:
            holding_to_ammend = pd.read_html(link, header=2)[-1]
        holding_to_ammend.index = holding_to_ammend["CUSIP"]
        ammendment_entries_links = ammendments[ammendments["cik"] == cik].get("infotable_link")
        for ammendment_link in ammendment_entries_links:
            ammendment_entry = pd.read_html(ammendment_link, header=2)[-1]
            ammendment_entry.index = ammendment_entry["CUSIP"]
            holding_to_ammend.update(ammendment_entry)
        ammended_holdings.append(holding_to_ammend)

    # Removing entries with ammendments from big dataframe
    all_hedgefunds_no_ammend = all_hedgefunds[all_hedgefunds["cik"].isin(to_ammend["cik"].unique().tolist()) == False]

    # Making a list of dataframes for holdings with no ammendments
    holdings_no_ammend_df = []
    for info_link in all_hedgefunds_no_ammend["infotable_link"]:
        try:
            df = pd.read_html(info_link, header=2)[-1]
            holdings_no_ammend_df.append(df)
        except:
            holdings_no_ammend_df.append("Unable to get holdings")
        time.sleep(0.1)

    # Reindex sequentially
    all_hedgefunds_no_ammend.index = range(0,len(holdings_no_ammend_df))

    # Save the files of non-ammended holdings per company per filing in name format "cik-date.csv"
    for i in range(0,len(holdings_no_ammend_df)):
        holdings = holdings_no_ammend_df[i]
        cik = all_hedgefunds_no_ammend["cik"][i]
        date = all_hedgefunds_no_ammend["date"][i].strftime("%Y%m%d")
        file_name = folder_path + "/" + str(cik) + "-" + str(date) + ".csv"
        try:
            holdings.to_csv(file_name)
        except AttributeError:
            pass

    # Reindex sequentially
    to_ammend.index = range(0,len(ammended_holdings))

    # Save the files of ammended holdings per company per filing
    for i in range(0,len(ammended_holdings)):
        holdings = ammended_holdings[i]
        cik = to_ammend["cik"][i]
        date = to_ammend["date"][i].strftime("%Y%m%d")
        file_name = folder_path + "/" + str(cik) + "-" + str(date) + ".csv"
        try:
            holdings.to_csv(file_name)
        except AttributeError:
            pass

clean_new_filings(company_files, "per_company_date_new")


# Establish path to folder with new holdings per company per date
new_filings_path = os.path.join(os.getcwd(), "per_company_date_new")
```

Comments in the code above help to explain what each part of the code is doing.

*Much of this process is a repeat from Step 1 of the project.*

### Parts 4/5: Calculate Changes from Last Filing & Update Database in MongoDB
Next, you would like to calculate the changes from the last filing and add these changes into your database on MongoDB. This can be done with the following code (there are comments within the code to demonstrate what each part of the code is doing).

```python
def calculate_new_changes(new_filings_path, folder_path):
    # Import function to get latest holding in database as dataframe
    from mongoinsertion import dict_to_df

    new_filings = os.listdir(new_filings_path)
    new_filings.remove(".DS_Store")

    # Get list of CIKs that have a new filing
    new_cik_list = []
    for file in new_filings:
        cik = file.split("-")[0]
        new_cik_list.append(cik)

    # Calculate the change dataframes based on latest holding in database and new holding being added
    from calculate_change import calculate_secondary
    import pandas as pd
    for cik in new_cik_list:
        latest_in_db = dict_to_df(cik)
        newer_filing_file = [file for file in new_filings if file.startswith(cik)]
        newer_filing = pd.read_csv(os.path.join(new_filings_path, newer_filing_file[0]))
        change_df = calculate_secondary(latest_in_db, newer_filing)
        new_date = newer_filing_file[0].split("-")[1].split(".")[0]
        change_df.to_csv(os.getcwd() + "/" + folder_path + "/" + str(cik) + "-" + str(
            new_date) + ".csv")

calculate_new_changes(new_filings_path, "change_dfs_new")
```
You can repeat Step 4 of this project to continuosly add 13-F filings as they are released and keep an up to date database on the long term holdings of the top hedge funds in the United States!
*If you'd like to see more or the entire code for updating the datbase click below.*

[Link to Code for Updating Database](https://github.com/amandasugiharto/stat359/blob/main/update_database.py)

<h1 align="center">
In Conclusion
</h1>

## Uses of Project
- Easily store, maintain, and update information about top hedge fund holdings
- Analyze long-term trends within the stock market
- Determine how certain stocks or hedge funds have performed over time
- Can add other change data to your database as you see fit (e.g. percentage of portfolio, total value of stocks, etc.) 
- Infer long-term investment strategies for yourself

### About the Authors

<p align="center">
<img src="https://github.com/austenw1899/PICTURES-FOR-BLOG/blob/main/Us.jpg?raw=true"/>
</p>

<h6 align="center">
Thanks for reading! Please consult the GitHub main branch for further clarification on the code or files.
</h6>















