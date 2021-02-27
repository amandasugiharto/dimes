# import json
import os


# this def main stuff is Windows specific, downloading the newest index file for Q1 2021
def main():
    import edgar
    edgar.download_index('/Users/Tim/PycharmProjects/Stat359Form13FParsing/index_files', 2021)


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
