import pandas as pd
import os
import bs4
import requests
import time

# df = pd.read_csv("13F/0000850529.txt", sep="|", index_col=False,
#                  names=["cik", "name", "form_type", "date", "txt", "html"])
#
# df2 = pd.read_csv("13F/0000872573.txt", sep="|", index_col=False,
#                  names=["cik", "name", "form_type", "date", "txt", "html"])
#
# df3 = df.append(df2)

# Specify path with 13F files per company and get list of files
root_path = "/Users/amandasugiharto/Documents/coding/PycharmProjects/pythonProject1/13F"
company_files = os.listdir(root_path)

# Initialize empty dataframe
all_hedgefunds = pd.DataFrame()

# Read in the data into a dataframe
for file in company_files:
    path = os.path.join("13F", file)
    hedgefund_item = pd.read_csv(path, sep="|", index_col=False,
                  names=["cik", "name", "form_type", "date", "txt", "html"])
    all_hedgefunds = all_hedgefunds.append(hedgefund_item)

# Clean the html and date columns
all_hedgefunds["html"] = "https://www.sec.gov/Archives/" + all_hedgefunds["html"]
all_hedgefunds["date"] = pd.to_datetime(all_hedgefunds["date"])

# Subset the data to more recent ones for less intensive parsing
# all_hedgefunds_new = all_hedgefunds[all_hedgefunds["date"] >= "2007-01-01"]
all_hedgefunds_new2 = all_hedgefunds[all_hedgefunds["date"] >= "2010-01-01"]

# html = requests.get("https://www.sec.gov/Archives/edgar/data/850529/0000850529-20-000010-index.html").text
# soup = bs4.BeautifulSoup(html, 'lxml')
# rows = soup.find_all('tr')[1:]
#
# soup.find("td", text="INFORMATION TABLE").find_previous_sibling("td").a.get("href")

# is_new = []
# for link in all_hedgefunds_new["html"]:
#     html = requests.get(link).text
#     soup = bs4.BeautifulSoup(html, 'lxml')
#     rows = soup.find_all('tr')[1:]
#     if soup.find("td", text="INFORMATION TABLE") != None:
#         is_new.append(True)
#     else:
#         is_new.append(False)
#     time.sleep(0.15)
#
# all_hedgefunds_new["has_info_table"] = is_new

# Get the html pages that have the "information table" format (latest format)
is_new2 = []
for link in all_hedgefunds_new2["html"]:
    html = requests.get(link).text
    soup = bs4.BeautifulSoup(html, 'lxml')
    rows = soup.find_all('tr')[1:]
    if soup.find("td", text="INFORMATION TABLE") != None:
        is_new2.append(True)
    else:
        is_new2.append(False)
    time.sleep(0.15)

# Append indicator on whether or not has the new format to original dataframe (with more recent filings)
all_hedgefunds_new2.loc[:,"has_info_table"] = is_new2
# Make a new dataframe with only the observations in the new format
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

# Add the information table links to the latest dataframe
all_hedgefunds_new2_infotable.loc[:,"infotable_link"] = infotable_links
all_hedgefunds_new2_infotable.loc[:,"infotable_link"] = "https://www.sec.gov" + all_hedgefunds_new2_infotable["infotable_link"]

# Checking how many extractions failed
all_hedgefunds_new2_infotable["infotable_link"].isna().sum()

# html = requests.get("https://www.sec.gov/Archives/edgar/data/1159159/000090266416007861/xslForm13F_X01/infotable.xml").text
# soup = bs4.BeautifulSoup(html, 'lxml')
# table = soup.find_all('table')[-1]

# table = pd.read_html("https://www.sec.gov/Archives/edgar/data/1159159/000090266416007861/xslForm13F_X01/infotable.xml", header = 2)[-1]

# Get the actual holdings from the information table links (will get list of dataframes)
holdings_df = []
for info_link in all_hedgefunds_new2_infotable["infotable_link"]:
    try:
        df = pd.read_html(info_link, header=2)[-1]
        holdings_df.append(df)
    except:
        holdings_df.append("Unable to get holdings")
    time.sleep(0.1)

# test = pd.read_fwf("https://www.sec.gov/Archives/edgar/data/850529/000085052913000010/llllll.txt")

# Add the holdings dataframes to each of the observations (not necessary, hence dropping it again)
all_hedgefunds_new2_infotable.loc[:,"holdings"] = holdings_df
all_hedgefunds_new2_infotable = all_hedgefunds_new2_infotable.drop(["holdings"], axis = 1)

# Reindex the dataframe with sequential index
all_hedgefunds_new2_infotable.index = range(0,len(holdings_df))

# Save the files per company per filing in name format "cik-date.csv"
for i in range(0,len(holdings_df)):
    holdings = holdings_df[i]
    cik = all_hedgefunds_new2_infotable["cik"][i]
    date = all_hedgefunds_new2_infotable["date"][i].strftime("%Y%m%d")
    file_name = "per_company_date/" + str(cik) + "-" + str(date) + ".csv"
    try:
        holdings.to_csv(file_name)
    except AttributeError:
        pass