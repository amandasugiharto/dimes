from collections import defaultdict
import re
import pandas as pd

with open("872573-20070930.txt") as myfile:  # open file
    form_info = myfile.read()  # Read the entire file to a string

dictionary = defaultdict(list)
input_string = form_info.split('\n')  # read string into array, each new line is a new entry

pattern = re.compile(r'(?:SH|PRN)')  # regular expression pattern looking for either SH or PRN on the line

table = []

for line in input_string:
    if pattern.search(line) is not None:  # Search for the pattern
        table.append(line)  # If found, add it to the table array

# print(table)

clean_table = table[2:]  # using slicing to perform removal of unwanted lines

# print(clean_table)


def parse_old13f(table_text):  # brute force parsing of substrings for the columns
    counter = 0
    for row in table_text:
        dictionary[counter].append(row[0:29].strip())
        dictionary[counter].append(row[30:44].strip())
        dictionary[counter].append(row[45:53].strip())
        dictionary[counter].append(row[54:63].strip())
        dictionary[counter].append(row[64:74].strip())
        dictionary[counter].append(row[75:77].strip())
        dictionary[counter].append(row[78:82].strip())
        counter = counter + 1


parse_old13f(clean_table)
old13f_dataframe = pd.DataFrame.from_dict(dictionary, orient='index',
                                          columns=["NAME OF ISSUER",
                                                   "TITLE OF CLASS",
                                                   "CUSIP",
                                                   "VALUE(x$1000)",
                                                   "SHRS OR PRN AMT",
                                                   "HR/PRN",
                                                   "PUT/CALL"])
#print(old13f_dataframe)

# for i in dictionary:
#    print(dictionary[i])

# print(len(dictionary))
