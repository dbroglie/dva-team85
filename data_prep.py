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
cols = ['bill_number', 'title', 'short_title​', 'sponsor_party​', 'introduced_date​', 'enacted​', 
        'vetoed​', 'cosponsors_count​', 'cosponsors_R', 'cosponsors_D', 'cosponsors_I', 
        'primary_subject​', 'summary_short​', 'actions_count​', 'votes_count']

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
    req_url = '/congress/v1/' + str(congress) + '/bills/' + bill_number + '.json'
    
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
    title = info['title'].replace(',', '')
    short_title = info['short_title'].replace(',', '')
    sponsor_party = info['sponsor_party']
    introduced_date = info['introduced_date']
    enacted = info['enacted']
    vetoed = info['vetoed']
    cosponsors_count = info['cosponsors']

    cosponsors_R = 0
    cosponsors_D = 0
    cosponsors_I = 0

    if cosponsors_count > 0:
        try:
            cosponsors_R = info['cosponsors_by_party']['R']
        except:
            pass
        
        try:
            cosponsors_D = info['cosponsors_by_party']['D']
        except:
            pass

        try:
            cosponsors_I = info['cosponsors_by_party']['I']
        except:
            pass

    primary_subject = info['primary_subject']
    summary_short = info['summary_short'].replace(',', '')
    actions_count = len(info['actions'])
    votes_count = len(info['votes'])

    bill = pd.DataFrame([[bill_number, title, short_title, sponsor_party, introduced_date, enacted, vetoed, cosponsors_count, 
            cosponsors_R, cosponsors_D, cosponsors_I, primary_subject, summary_short, actions_count, votes_count]], columns=cols)
            
    bill_details_df = bill_details_df.append(bill, ignore_index=True)

# close connection
connection.close()

# export results to csv
bill_details_df.to_csv("bill_details_116.csv")
