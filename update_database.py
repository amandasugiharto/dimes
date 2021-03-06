# import json
import os


# this def main stuff is Windows specific, downloading the newest index file for Q1 2021
def main():
    import edgar
    edgar.download_index(os.path.join(os.getcwd(),"index_files_new"), 2021)

if __name__ == '__main__':
    main()


# open the json with the company names and CIKs
# with open('/Users/Tim/PycharmProjects/Stat359Form13FParsing/companies-13f.json') as f:
#    data = json.load(f)

# some company names changed (OZ MANAGEMENT LP to Sculptor Cpital LP and BRIDGEWATER CAPITAL ADVISORS INC to PUNCH & ASSOCIATES INVESTMENT MANAGEMENT, INC
# also "MFP INVESTORS LLC", "1105685" stopped filing 13-Fs in 2008, so database shrinks down to only 46 companies total
data = [['Schonfeld Strategic Advisors LLC', '1665241'], ['Hitchwood Capital Management LP', '1611613'], ['BAMCO INC /NY/', '1017918'], ['PUNCH & ASSOCIATES INVESTMENT MANAGEMENT, INC', '1238990'], ['BlueCrest Capital Management Ltd', '1610880'], ['Duquesne Family Office LLC', '1536411'], ['TCI Fund Management Ltd', '1647251'], ['SB INVESTMENT ADVISERS (UK) LTD', '1731509'], ['VISTA EQUITY PARTNERS III', '1569532'], ['TWO SIGMA ADVISERS', '1478735'], ['Fisher Asset Management', '850529'], ['KING STREET CAPITAL MANAGEMENT', '1218199'], ['York Capital Management Global Advisors', '1480532'], ['ABRAMS CAPITAL MANAGEMENT', '1358706'], ['Third Point LLC', '1040273'], ['TRIAN FUND MANAGEMENT', '1345471'], ['HILLHOUSE CAPITAL ADVISORS', '1762304'], ['TIGER MANAGEMENT L.L.C.', '1027451'], ['MOORE CAPITAL MANAGEMENT', '1448574'], ['GLENVIEW CAPITAL MANAGEMENT', '1138995'], ['Appaloosa LP', '1656456'], ['SOROS FUND MANAGEMENT LLC', '1029160'], ['PAULSON & CO. INC.', '1035674'], ['RENAISSANCE TECHNOLOGIES LLC', '1037389'], ['LONE PINE CAPITAL LLC', '1061165'], ['BERKSHIRE HATHAWAY INC', '1067983'], ['VIKING GLOBAL INVESTORS LP', '1103804'], ['SANDELL ASSET MANAGEMENT CORP', '1140474'], ['TIGER GLOBAL MANAGEMENT LLC', '1167483'], ['PERCEPTIVE ADVISORS LLC', '1224962'], ['MILLENNIUM MANAGEMENT LLC', '1273087'], ['CAXTON CORP', '1388551'], ['CAXTON ASSOCIATES LP', '872573'], ['TUDOR INVESTMENT CORP ET AL', '923093'], ['COOPERMAN LEON G', '898382'], ['Sculptor Capital LP', '1054587'], ['GREENLIGHT CAPITAL INC', '1079114'], ['JANA PARTNERS LLC', '1159159'], ['PLATINUM INVESTMENT MANAGEMENT LTD', '1256071'], ['CITADEL ADVISORS LLC', '1423053'], ['OAKTREE CAPITAL MANAGEMENT LP', '949509'], ['MFP INVESTORS LLC', '1105685'], ['CHILTON INVESTMENT CO LLC', '1332632'], ['TWO SIGMA INVESTMENTS LLC', '1179392'], ['HIGHBRIDGE CAPITAL MANAGEMENT LLC', '919185'], ['ICAHN CARL C', '921669'], ['BAUPOST GROUP LLC/MA', '1061768']]



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

# Define function to calculate new change dataframes and save to csv
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