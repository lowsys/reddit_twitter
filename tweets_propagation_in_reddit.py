import time
import requests
import os
from datetime import datetime
import pandas as pd
import numpy as np

# this function downloads data about tweets from chosen subreddit and timeframe
# downloaded data are saved to csv file
def download_reddit_data(start_date, end_date, subreddit_name):

    # dataframe for dictionary data
    df_dictionary = pd.DataFrame()

    # dataframe for list data
    df_list = pd.DataFrame()

    reddit_list = [
        [
            'reddit_id',
            'reddit_title',
            'reddit_timestamp',
            'reddit_date',
            'reddit_link',
            'reddit_twitter_id',
        ]
    ]
    reddit_dict = {}

    # file path to windows desktop
    FILENAME_PATH = f'{os.environ["USERPROFILE"]}\\Desktop\\'

    START_DATE = datetime.strptime(start_date, '%Y, %m, %d')
    END_DATE = datetime.strptime(end_date, '%Y, %m, %d')

    # API needs int value, timestamp is float by default
    after_timestamp = int(START_DATE.timestamp())
    before_timestamp = int(END_DATE.timestamp())

    subreddit = subreddit_name
    limit = 100

    while after_timestamp < before_timestamp:

        # pushift API has pretty high rate limits, time.sleep() might be needed
        time.sleep(10)
        print('dates:')
        print(
            f'{datetime.utcfromtimestamp(after_timestamp)} <-> '
            f'{datetime.utcfromtimestamp(before_timestamp)}'
        )
        url = (
            f'https://api.pushshift.io/reddit/search/submission?&subreddit={subreddit}&after={after_timestamp}'
            f'&before={before_timestamp}&limit={limit}'
        )
        request = requests.get(url)

        if request.status_code != 200:
            raise Exception(request.status_code)
        json_response = request.json()

        # two ways of storing reddit data. As python list and pyton dictionary
        for post in json_response['data']:
            if post['url'].startswith('https://twitter.com') or post['url'].startswith(
                'https://mobile.twitter.com'
            ):

                # storing data as python list
                sub_reddit_list = [
                    post['id'],
                    post['title'],
                    post['created_utc'],
                    datetime.utcfromtimestamp(post['created_utc']),
                    post['url'],

                    # extract twitter id from the twitter url
                    post['url'][
                        post['url'].find('status/')
                        + 7 : post['url'].find('status/')
                        + 26
                    ],
                ]
                reddit_list.append(sub_reddit_list)
                new_row = pd.DataFrame(reddit_list)
                sub_reddit_list = []
                df_list = pd.concat([df_list, new_row])
                reddit_list = []

                # storing data as python dictionary
                reddit_dict.update(
                    {
                        '1_id': post['id'],
                        '2_title': post['title'],
                        '3_created timestamp': post['created_utc'],
                        '4_created utc': datetime.utcfromtimestamp(post['created_utc']),
                        '5_post_url': post['url'],
                        # extract twitter id from the twitter url
                        '6_twitter_id': post['url'][
                            post['url'].find('status/')
                            + 7 : post['url'].find('status/')
                            + 26
                        ],
                    }
                )
                new_dict_row = pd.DataFrame([reddit_dict])
                df_dictionary = pd.concat([df_dictionary, new_dict_row])
                reddit_dict = {}

            # saving every loop pass. Pushshift is not very reliable.
            df_dictionary.to_csv(
                FILENAME_PATH
                + f'{start_date.replace(", ", "-")}_{end_date.replace(", ", "-")}_'
                f'{subreddit_name}_dict.csv',
                mode='a',
                header=False,
            )
            df_list.to_csv(
                FILENAME_PATH
                + f'{start_date.replace(", ", "-")}_{end_date.replace(", ", "-")}_'
                f'{subreddit_name}_list.csv',
                mode='a',
                header=False,
            )

            # clear dataframe after each loop
            df_dictionary = df_dictionary[0:0]
            df_list = df_list[0:0]

        # end of the loop, start date (API "after") is equal end date (API "before").
        if post['created_utc'] == after_timestamp:
            print('equal dates, i am done')
            break
        else:
            after_timestamp = post['created_utc']

# this function search for tweet date and compares it with subreddit post date
# tweet date and time difference is saved to a new csv file
def calculate_propagation_time(file_name):
    # BEARER_TOKEN is a token you can find after registration for twitter api.
    # Detalied guide: https://towardsdatascience.com/searching-for-tweets-with-python-f659144b225f
    BEARER_TOKEN = 'YOUR_TOKEN'

    # define search twitter function
    headers = {'Authorization': f'Bearer {BEARER_TOKEN}'}

    # tweet field "text" is not used, can be for sanity check
    tweet_fields = 'id,created_at,text'

    df = pd.read_csv(f'{os.environ["USERPROFILE"]}\\Desktop\\{file_name}', header=None)

    # add reddit headers to df
    df.columns = [
        'id',
        'reddit_id',
        'reddit_title',
        'reddit_timestamp',
        'reddit_date',
        'reddit_link',
        'reddit_twitter_id',
    ]
    
    # add twitter headers to df
    df['twitter_timestamp'] = ''
    df['difference_in_minutes'] = ''

    # take tweet id from each line
    # and query twitter API for tweet_fields for this tweet
    row_index = 0
    for twitterId in df.iloc[:, 6]:

        # tweeter API timer depends querries/15 minutes
        time.sleep(1)
        url = f'https://api.twitter.com/2/tweets?ids={twitterId}&tweet.fields={tweet_fields}'
        response = requests.request('GET', url, headers=headers)

        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        response_twitter = response.json()

        # in case of deleted tweet JSON starts with 'error', not 'data'
        for JSON_root_value in response_twitter:
            if JSON_root_value == 'data':

                # save tweet date as timestamp for each reddit post row
                for twitter_post in response_twitter['data']:
                    tweet_date = twitter_post['created_at']
                    tweet_date_stamp = int(
                        datetime.strptime(
                            tweet_date, '%Y-%m-%dT%H:%M:%S.%fZ'
                        ).timestamp()
                    )
                    df.iloc[row_index, 7] = tweet_date_stamp
                    
                    # calculate propagation time in minutes
                    df.iloc[row_index, 8] = ((
                        int(df.iloc[row_index, 3]) - int(df.iloc[row_index, 7])
                    ) / 60) - 60 # '-60' is a crude way to handle timezone
            
                print(f'row index: {row_index} tweet date: {tweet_date}')
                row_index = row_index + 1

    # save file with twitter timestamp and difference between tweet and sharing it on reddit
    df.to_csv(f'{os.environ["USERPROFILE"]}\\Desktop\\propagation_{file_name}')

# this function poerform basic analyses, just for sanity check
def time_diff_analyses(file_name):
    df = pd.read_csv(f'{os.environ["USERPROFILE"]}\\Desktop\\{file_name}')

    #general analyses
    print(df['difference_in_minutes'].astype(float).describe().apply('{0:.2f}'.format))

# sample use. analyse how fast tweets were share in subreddit polska
# between 2021.12.01 and 2021.12.04 
download_reddit_data('2021, 12, 1', '2021, 12, 4', 'polska')
calculate_propagation_time('2021-12-1_2021-12-4_polska_dict.csv')
time_diff_analyses('propagation_2021-12-1_2021-12-4_polska_dict.csv')
