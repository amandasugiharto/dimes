import pandas as pd
import os
import itertools

root_path = "/Users/amandasugiharto/Documents/coding/PycharmProjects/pythonProject1/per_company_date"
company_holdings = sorted(os.listdir(root_path))
old_holdings = pd.read_csv(os.path.join(root_path, company_holdings[1]), index_col = 0)
new_holdings = pd.read_csv(os.path.join(root_path, company_holdings[2]), index_col = 0)

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
all_holdings = pd.DataFrame({"CUSIP":union_holdings,
                             "status":list(itertools.repeat("nan", len(union_holdings))) })
all_holdings.index = all_holdings["CUSIP"]

# old_holdings_subset = old_holdings.loc[:,["(x$1000)", "PRN AMT"]]
# Get a subset of the holdings still there and calculate the change on value and shares held
same_df_subset = same_df.loc[:,["NAME OF ISSUER", "(x$1000)", "PRN AMT"]]

change_df = same_df_subset.join(old_holdings.loc[:,["(x$1000)", "PRN AMT"]], on = "CUSIP", lsuffix = "_old")
change_df.loc[:,"(x$1000)_change"] = change_df.loc[:,"(x$1000)"] - change_df.loc[:,"(x$1000)_old"]
change_df.loc[:,"PRN AMT_change"] = change_df.loc[:,"PRN AMT"] - change_df.loc[:,"PRN AMT_old"]

# Get a list of status of each holding and append to big dataframe
status_list = []
for holding in union_holdings:
    if holding in list(same_df["CUSIP"]):
        status_list.append("Not New")
    elif holding in list(new_df["CUSIP"]):
        status_list.append("New Holding")
    elif holding in list(sold_out_df["CUSIP"]):
        status_list.append("Sold Out")
all_holdings.loc[:,"status"] = status_list

# Join the change, new, and sold out dataframes to the main one
all_holdings = all_holdings[["status"]].join(change_df, on = "CUSIP")
all_holdings = all_holdings.join(sold_out_df.loc[:,["NAME OF ISSUER", "(x$1000)", "PRN AMT"]], rsuffix = "_soldout", on = "CUSIP")
all_holdings = all_holdings.join(new_df.loc[:,["NAME OF ISSUER", "(x$1000)", "PRN AMT"]], rsuffix = "_new", on = "CUSIP")


# Fill missing values from previous joins
all_holdings.loc[:,"NAME OF ISSUER"] = all_holdings["NAME OF ISSUER"].fillna(all_holdings["NAME OF ISSUER_new"])
all_holdings.loc[:,"NAME OF ISSUER"] = all_holdings["NAME OF ISSUER"].fillna(all_holdings["NAME OF ISSUER_soldout"])
all_holdings.loc[:,"(x$1000)"] = all_holdings["(x$1000)"].fillna(all_holdings["(x$1000)_new"])
all_holdings.loc[:,"PRN AMT"] = all_holdings["PRN AMT"].fillna(all_holdings["PRN AMT_new"])
all_holdings.loc[:,"(x$1000)_old"] = all_holdings["(x$1000)_old"].fillna(all_holdings["(x$1000)_soldout"])
all_holdings.loc[:,"PRN AMT_old"] = all_holdings["PRN AMT_old"].fillna(all_holdings["PRN AMT_soldout"])

# Drop redundant columns
all_holdings = all_holdings.drop(["NAME OF ISSUER_new", "NAME OF ISSUER_soldout", "(x$1000)_new",
                                  "PRN AMT_new", "(x$1000)_soldout", "PRN AMT_soldout"], axis = 1)

all_holdings.to_csv("/Users/amandasugiharto/Documents/coding/PycharmProjects/pythonProject1/change_test.csv")
