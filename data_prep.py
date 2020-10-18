import csv
import pandas as pd
import http.client
import json

#### prep core bill data ####

# read in csv
bills_df = pd.read_csv("HSall_rollcalls.csv")

# drop unnecessary columns
cols_to_drop = ['nominate_mid_1', 'nominate_mid_2', 'nominate_spread_1', 'nominate_spread_2', 'nominate_log_likelihood']
bills_df = bills_df.drop(cols_to_drop, axis=1)

# drop rows where bill number is null
bills_df = bills_df[bills_df['bill_number'].notnull()]

# filter on single congress for testing purposes
# TODO update this later
bills_df = bills_df[bills_df['congress'] > 115]
bills_dedup = bills_df.filter(['congress', 'bill_number'], axis=1)
bills_dedup.drop_duplicates(inplace=True)


#### retrieve additional bill data ####

# create new dataframe
# TODO figure out what columns we want to create here, will merge with core DF later 
cols = ['congress', 'bill_number', 'title', 'sponsor_party', 'introduced_date', 'house_passage_vote', 'senate_passage_vote', 'subjects']
bill_details_df = pd.DataFrame(columns=cols)

# connect to API
connection = http.client.HTTPSConnection('api.propublica.org')
api_key = '1oV367lUSPOgewb3yAgQXKASRyAjYNuEg2pNpeLR'
headers = {'X-API-Key': '1oV367lUSPOgewb3yAgQXKASRyAjYNuEg2pNpeLR'}

# iterate over bills
for index, row in bills_dedup.iterrows():
    congress = row['congress']
    bill_number = row['bill_number']

    # create request url
    req_url = '/congress/v1/' + str(congress) + '/bills/' + bill_number + '/subjects.json'

    # connect to API
    connection = http.client.HTTPSConnection('api.propublica.org')

    # make get request
    connection.request('GET', req_url, headers=headers)

    # create json object from response
    response = json.loads(connection.getresponse().read())

    # check that there wasn't an error - check status on the API reponse
    if response['status'] != 'OK':
        errors = ''.join(json.dumps(response['errors']))
        print(bill_number + ': ' + errors)
        continue

    info = response['results'][0]

    # extract data
    title = info['title']
    sponsor_party = info['sponsor_party']
    introduced_date = info['introduced_date']
    house_passage_vote = info['house_passage_vote']
    senate_passage_vote = info['senate_passage_vote']

    subjects = info['subjects']

    subject_concat = ''
    for i in range(len(subjects)):
        subject_concat = subject_concat + subjects[i]['name'] + ';'

    if len(subject_concat) > 0:
        subject_concat = subject_concat[:-1]

    bill = pd.DataFrame([[congress, bill_number, title, sponsor_party, introduced_date, house_passage_vote, senate_passage_vote, subject_concat]], columns=cols)
    bill_details_df = bill_details_df.append(bill, ignore_index=True)

# close connection
connection.close()

# export results to csv
bill_details_df.to_csv("bill_details_116.csv")