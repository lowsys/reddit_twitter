import requests
import os
from datetime import datetime
import pandas as pd
import twitter_credentials

def download_reddit_data(start_date, end_date, subreddit_name):
    
    # dataframe for dictionary data
    df_dictionary = pd.DataFrame()
    
    # dataframe for list data
    df_list = pd.DataFrame()
    
    reddit_list = [
        ['reddit_id', 'reddit_title', 'reddit_timestamp', 'reddit_date', 'reddit_link', 'reddit_twitter_id']]
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
        print("dates:")
        print(f'{datetime.utcfromtimestamp(after_timestamp)} <-> '
              f'{datetime.utcfromtimestamp(before_timestamp)}')
        url = f"https://api.pushshift.io/reddit/search/submission?&subreddit={subreddit}&after={after_timestamp}" \
              f"&before={before_timestamp}&limit={limit}"
        request = requests.get(url)
        if request.status_code != 200:
            raise Exception(request.status_code)
        json_response = request.json()

        # two ways of storing reddit data. As python list and pyton dictionary
        for post in json_response["data"]:
            if post['url'].startswith("https://twitter.com") or post['url'].startswith("https://mobile.twitter.com"):

                # storing data as python list
                sub_reddit_list = [post['id'],
                                    post["title"],
                                    post["created_utc"],
                                    datetime.utcfromtimestamp(post["created_utc"]),
                                    post['url'],
                                    post['url'][post['url'].find('status/') + 7:post['url'].find('status/') + 26]
                                    ]
                reddit_list.append(sub_reddit_list)
                new_row = pd.DataFrame(reddit_list)
                sub_reddit_list = []
                df_list = pd.concat([df_list, new_row])
                reddit_list = []

                # storing data as python dictionary does not have much sense if saving line by line
                reddit_dict.update({
                    '1_id': post['id'],
                    '2_title': post["title"],
                    '3_created timestamp': post["created_utc"],
                    '4_created utc': datetime.utcfromtimestamp(post["created_utc"]),
                    '5_post_url': post['url'],
                    '6_twitter_id': post['url'][post['url'].find('status/') + 7:post['url'].find('status/') + 26]}
                )
                new_dict_row = pd.DataFrame([reddit_dict])
                df_dictionary = pd.concat([df_dictionary, new_dict_row])
                reddit_dict = {}


            # saving every loop pass. Pushshift is not very reliable
            df_dictionary.to_csv(FILENAME_PATH + f'{start_date.replace(", ", "-")}_{end_date.replace(", ", "-")}_'
                                                 f'{subreddit_name}dict.csv', mode='a', header=False)
            df_list.to_csv(FILENAME_PATH + f'{start_date.replace(", ", "-")}_{end_date.replace(", ", "-")}_'
                                                 f'{subreddit_name}list.csv', mode='a', header=False)

            # clear dataframe after each loop
            df_dictionary = df_dictionary[0:0]
            df_list = df_list[0:0]

        # end of the loop, start date is equal end date.
        if post["created_utc"] == after_timestamp:
            print('equal dates, i am done')
            break
            # quit()
        else:
            after_timestamp = post["created_utc"]

def calculate_propagation_time(file_name):
    # BEARER_TOKEN is a token you can find after registration for twitter api.
    # Detalied guide: https://towardsdatascience.com/searching-for-tweets-with-python-f659144b225f
    BEARER_TOKEN = twitter_credentials.twitter_token

    # define search twitter function
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    tweet_fields = "id,created_at,text"

    df = pd.read_csv(f'{os.environ["USERPROFILE"]}\\Desktop\\{file_name}', header=None)
    print(df)

    # add reddit headers to df
    df.columns = ['id', 'reddit_id', 'reddit_title', 'reddit_timestamp', 'reddit_date', 'reddit_link',
                  'reddit_twitter_id']

    # add twitter headers to df
    df['twitter_timestamp'] = ''
    df['difference_in_minutes'] = ''

    # take tweet id from each line (starting with row_index = 0) and query twitter API for tweet_fields from this tweet
    row_index = 1
    for twitterId in df.iloc[1:, 6]:
        print('idtweeta', twitterId)
        url = f"https://api.twitter.com/2/tweets?ids={twitterId}&tweet.fields={tweet_fields}"
        response = requests.request("GET", url, headers=headers)

        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        response_twitter = response.json()
        print(response_twitter)

        # save tweet date as timestamp for each reddit post row
        for twitter_post in response_twitter["data"]:
            print(twitter_post['created_at'])
            tweet_date = twitter_post['created_at']
            tweet_date_stamp = int(datetime.strptime(tweet_date, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
            df.iloc[row_index, 7] = tweet_date_stamp
            # calculate propagation time in minutes
            df.iloc[row_index, 8] = (int(df.iloc[row_index, 3]) - int(df.iloc[row_index, 7])) / 60
            row_index = row_index + 1
    print(df)
    df = df.drop(labels=0, axis=0)
    print(df)
    print(df['difference_in_minutes'].describe())
download_reddit_data('2022, 2, 1', '2022, 2, 3', 'polska')

calculate_propagation_time('2022-2-1_2022-2-3_polskalist.csv')