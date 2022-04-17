# reddit_twitter
How long does it take a tweet to be shared on reddit?
This script is an answer to the question above. Just choose period of time and subreddit name.
You will get answer with .csv file for even more advanced analyses on tweets in reddit.

Methodology:
Source of reddit post is Pushshift API which collects doubled and deleted posts.

Advice:
Check Pushift and twitter API’s limits, on large amount of data you may encounter too many requests error.

Pushsigt API documentation: https://github.com/pushshift/api

New to Pushift? https://www.reddit.com/r/pushshift/comments/bcxguf/new_to_pushshift_read_this_faq/

Twitter API: https://developer.twitter.com/en/docs/twitter-api

Sample use of Twitter API: https://towardsdatascience.com/searching-for-tweets-with-python-f659144b225f

Example of use:
Find and calculate how fast tweets are shared in subreddit "polska" between the first and fourth December 2021:

download_reddit_data('2021, 12, 1', '2021, 12, 4', 'polska')

calculate_propagation_time('2021-12-1_2021-12-4_polska_dict.csv')

time_diff_analyses('propagation_2021-12-1_2021-12-4_polska_dict.csv')

Example output (values in minutes):
count       8.00, 

mean     1362.43, 

std      1309.60, 

min       141.03, 

25%       194.12, 

50%      1163.99, 

75%      2332.30, 

max      2980.72

Name: difference_in_minutes, dtype: object

Example answer based on example output:
There were 8 tweets shared on subreddit polska. The fastest share took 141.03 minutes, the longest - 2980.72 minutes.
On average, tweets are shared after 1362.43 minutes and median is 1163.99 minutes.
