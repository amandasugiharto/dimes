import json
import os


# this def main stuff is Windows specific, downloading the newest index file for Q1 2021
def main():
    import edgar
    edgar.download_index('/Users/Tim/PycharmProjects/Stat359Form13FParsing/index_files', 2021)


if __name__ == '__main__':
    main()

# open the json with the company names and CIKs
with open('/Users/Tim/PycharmProjects/Stat359Form13FParsing/companies-13f.json') as f:
    data = json.load(f)


# function to find the correct companies
def find_companies(company_list):
    for company in company_list:
        infile = open('/Users/Tim/PycharmProjects/Stat359Form13FParsing/index_files/2021-QTR1.tsv',
                      'r')  # This line is referencing the new index file
        outfile = open('/Users/Tim/PycharmProjects/Stat359Form13FParsing/index_files/companies/' + company[1] + '.txt',
                       'w')
        for line in infile:
            if line.find(company[0]) != -1:
                outfile.write(line)
            else:
                continue
        infile.close()
        outfile.close()


# function to find the correct forms
def find_forms(form_string):
    directory = '/Users/Tim/PycharmProjects/Stat359Form13FParsing/index_files/companies'
    for filename in os.listdir(directory):
        infile = open(os.path.join(directory, filename), 'r')
        outfile = open('/Users/Tim/PycharmProjects/Stat359Form13FParsing/index_files/' + form_string + '/' + filename,
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
find_forms('13F')
