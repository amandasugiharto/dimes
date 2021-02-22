import pandas as pd
import os
import itertools

root_path = "/Users/amandasugiharto/Documents/coding/PycharmProjects/pythonProject1/per_company_date"
company_holdings = sorted(os.listdir(root_path))


# Function to calculate secondary dataframe from old and new filings

def calculate_secondary(old_filing, new_filing):
    # Read in the files
    old_holdings = pd.read_csv(os.path.join(root_path, old_filing), index_col=0)
    new_holdings = pd.read_csv(os.path.join(root_path, new_filing), index_col=0)

    # Get lists of holdings in previous time period and this time period
    old_holdings_list = old_holdings["CUSIP"].to_list()
    new_holdings_list = new_holdings["CUSIP"].to_list()

    # Reindex dataframes using CUSIP
    old_holdings.index = old_holdings["CUSIP"]
    new_holdings.index = new_holdings["CUSIP"]

    # Determine what the new holdings are
    new_holding = []
    for holding in new_holdings_list:
        if holding not in old_holdings_list:
            new_holding.append(True)
        else:
            new_holding.append(False)
    new_holdings.loc[:, "new"] = new_holding

    # Determine which holdings from the previous period were sold out
    sold_out = []
    for holding in old_holdings_list:
        if holding not in new_holdings_list:
            sold_out.append(True)
        else:
            sold_out.append(False)
    old_holdings.loc[:, "soldout_next"] = sold_out

    # Make dataframes with of the different holdings in the 3 scenarios and reindex
    sold_out_df = old_holdings[old_holdings["soldout_next"] == True]
    new_df = new_holdings[new_holdings["new"] == True]
    same_df = new_holdings[new_holdings["new"] == False]

    sold_out_df.index = sold_out_df["CUSIP"]
    new_df.index = new_df["CUSIP"]
    same_df.index = same_df["CUSIP"]

    # Get a list of all past and current holdings
    union_holdings = list(set().union(new_holdings_list, old_holdings_list))

    # Initialize a dataframe of all holdings across both filings and reindex
    all_holdings = pd.DataFrame({"CUSIP": union_holdings,
                                 "status": list(itertools.repeat("nan", len(union_holdings)))})
    all_holdings.index = all_holdings["CUSIP"]

    # Get a subset of the holdings still there and calculate the change on value and shares held
    same_df_subset = same_df.loc[:, ["NAME OF ISSUER", "(x$1000)", "PRN AMT"]]

    change_df = same_df_subset.join(old_holdings.loc[:, ["(x$1000)", "PRN AMT"]], on="CUSIP", lsuffix="_old")
    change_df.loc[:, "(x$1000)_change"] = change_df.loc[:, "(x$1000)"] - change_df.loc[:, "(x$1000)_old"]
    change_df.loc[:, "PRN AMT_change"] = change_df.loc[:, "PRN AMT"] - change_df.loc[:, "PRN AMT_old"]

    # Get a list of status of each holding and append to big dataframe
    status_list = []
    for holding in union_holdings:
        if holding in list(same_df["CUSIP"]):
            status_list.append("Not New")
        elif holding in list(new_df["CUSIP"]):
            status_list.append("New Holding")
        elif holding in list(sold_out_df["CUSIP"]):
            status_list.append("Sold Out")
    all_holdings.loc[:, "status"] = status_list

    # Join the change, new, and sold out dataframes to the main one
    all_holdings = all_holdings[["status"]].join(change_df, on="CUSIP")
    all_holdings = all_holdings.join(sold_out_df.loc[:, ["NAME OF ISSUER", "(x$1000)", "PRN AMT"]], rsuffix="_soldout",
                                     on="CUSIP")
    all_holdings = all_holdings.join(new_df.loc[:, ["NAME OF ISSUER", "(x$1000)", "PRN AMT"]], rsuffix="_new",
                                     on="CUSIP")

    # Fill missing values from previous joins
    all_holdings.loc[:, "NAME OF ISSUER"] = all_holdings["NAME OF ISSUER"].fillna(all_holdings["NAME OF ISSUER_new"])
    all_holdings.loc[:, "NAME OF ISSUER"] = all_holdings["NAME OF ISSUER"].fillna(
        all_holdings["NAME OF ISSUER_soldout"])
    all_holdings.loc[:, "(x$1000)"] = all_holdings["(x$1000)"].fillna(all_holdings["(x$1000)_new"])
    all_holdings.loc[:, "PRN AMT"] = all_holdings["PRN AMT"].fillna(all_holdings["PRN AMT_new"])
    all_holdings.loc[:, "(x$1000)_old"] = all_holdings["(x$1000)_old"].fillna(all_holdings["(x$1000)_soldout"])
    all_holdings.loc[:, "PRN AMT_old"] = all_holdings["PRN AMT_old"].fillna(all_holdings["PRN AMT_soldout"])

    # Drop redundant columns
    all_holdings = all_holdings.drop(["NAME OF ISSUER_new", "NAME OF ISSUER_soldout", "(x$1000)_new",
                                      "PRN AMT_new", "(x$1000)_soldout", "PRN AMT_soldout"], axis=1)

    # Return dataframe
    return all_holdings

# Get list of CIKs
cik_list = []
for company in company_holdings[1:]:
    separated = company.split("-")
    cik = separated[0]
    cik_list.append(cik)

cik_list_unique = list(set(cik_list))

# Count how many filings we have for each hedgefund (for debugging purposes only for now)
filings_count = []
for cik in cik_list_unique:
    num_filings = cik_list.count(cik)
    filings_count.append(num_filings)
filings_count_df = pd.DataFrame({"cik":cik_list_unique, "num_filings":filings_count})


# Use the function on every pair of filings per company and save dataframes
for cik in cik_list_unique:
    filing_list = [filing for filing in company_holdings if filing.startswith(cik)]
    filing_list_length = len(filing_list)
    for i in range(0,filing_list_length-1):
        df = calculate_secondary(filing_list[i], filing_list[i+1])
        df.to_csv("/Users/amandasugiharto/Documents/coding/PycharmProjects/pythonProject1/change_dfs/" + cik + "-" + str(i) + ".csv")
