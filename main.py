import os
from math import ceil
import json
import configparser
from TwitterAPI import TwitterAPI, TwitterPager
from time import sleep, time
from datetime import datetime, timedelta
from deepdiff import DeepDiff
import pandas as pd

# Set target list here. Must be public
DEFAULT_LIST = 1523603961562812418

# Store sensitive keys in config.ini file in same directory
configs = configparser.ConfigParser()
configs.read('./config.ini')
keys = configs['TWITTER']
consumer_key = keys['CONSUMER_KEY']
consumer_secret = keys['CONSUMER_SECRET']
access_token = keys['ACCESS_TOKEN']
access_secret = keys['ACCESS_SECRET']

client = TwitterAPI(consumer_key, consumer_secret, access_token, access_secret, api_version="2")

def twitter_uid(username):
    # returns twitter user id from username
    return client.request(f'users/by/username/:{username}').json()['data']['id']


def pull_following(uid, username):
    # Creates a json file containing list of followers
    print(f"Please wait while we fetch {username}'s follows")
    params = {
            "max_results": 999,
            "user.fields": "id,username,created_at",
        }
    pager = TwitterPager(client, f"users/:{uid}/following", params)
    followers = list(pager.get_iterator())

    save_json(followers, os.path.join("data", f"{todays_date}_{username}.json"))
    print("#" * 60)


def following_count(uid):
    # Returns number of followers.
    # To inform effort to work within rate limits
    query_result = client.request(f"users/:{uid}", params={"user.fields": "public_metrics"}).json()
    following = query_result['data']['public_metrics']['following_count']
    return following


def list_members(lid):
    # Pulls the ids and usernames of members of a list (lid).
    # Returns list of dictionaries
    query_result = client.request(f"lists/:{lid}/members").json()['data']
    return query_result



def save_json(data, save_name):
    # save followers
    out_file = open(save_name, "w")
    json.dump(data, out_file, indent=6, sort_keys=True)
    out_file.close()
    print(f"Saved to file {save_name}")


def batch_collect_following(target_list=DEFAULT_LIST):
    # iterates through the members of a twitter list to pull their following and save the results to json
    # file while respecting twitter call limits
    # could the iterating twitter list in this function and below be coded as a helper?
    list = list_members(target_list)

    calls = 0
    for account in list:
        # Prints username and an amount of dashes to create a break in the print statements
        account_follower_count = following_count(account['id'])
        print(f"### {account['username']} is following {account_follower_count} ###")
        # how many api calls required assuming limit of 1000 per call. Ceil rounds up result
        calls_required = ceil(account_follower_count / 1000)
        if calls_required + calls > 15:
            restart_time = datetime.now() + timedelta(minutes=15)
            print (f"Sleeping for 15 minutes to not upset Twitter limit gods. Will restart at {restart_time.strftime('%H:%M')}")
            sleep(900)
            calls = 0
        print(f"Recording who {account['username']}'s following")
        pull_following(account['id'], account['username'])
        calls += calls_required


def compare_following(username, days=1):
    # compare an accounts json list of followers
    print(f"Checking for changes in accounts {username} is following over the last {days} day(s)")
    todays_file = open(os.path.join('data', f"{todays_date}_{username}.json"))
    try:
        prev_file = open(os.path.join('data', f"{todays_date-timedelta(days=days)}_{username}.json"))
        prev_list = json.load(prev_file)
        todays_list = json.load(todays_file)
        results = DeepDiff(prev_list, todays_list, ignore_order=True)
        return results
    except FileNotFoundError:
        print(f"***WARNING*** No previous data pull for {username}. This may be because the account was added to the twitter list in"
              f"the last {days} day(s). Otherwise there may be a problem")
        return False



def batch_compare_following(target_list=DEFAULT_LIST, days=1):
    # returns list of dictionaries containing all new follows having iterated a twitter list with compare_following()
    list = list_members(target_list)

    aggregated_differences = []

    for member in list:
        differences = compare_following(member['username'], days=days)
        if differences:
            try:
                for line in differences['iterable_item_added']:
                    dict = differences['iterable_item_added'][line]
                    dict['originator'] = member['username']
                    aggregated_differences.append(dict)
                print(f"{member['username']} followed {len(differences['iterable_item_added'])} new accounts")
            except KeyError:
                print(f"No new follows by {member['username']}")

    return aggregated_differences


def findings(data):

    # create dataframe from data
    df = pd.DataFrame.from_dict(data)
    # create series of new follows and filter series to only include follows > 1
    new_follows = df['username'].value_counts().loc[lambda x: x > 1]

    who_follows = {}
    #iterate over remaining series to show number of new follows and the accounts
    for index, value in new_follows.items():
        who_follows[index] = df[df.username == index]['originator'].tolist()

    print("#" * 60)
    print("Trending Sol Follows \n")
    for key in who_follows:
        follow_list = (who_follows[key])
        print(f"{key} x {len(follow_list)}:")
        print(*follow_list, sep=", ")
        print("\n")


if __name__ == '__main__':
    startTime = time()
    todays_date = datetime.now().date() -timedelta(days=1)
    batch_collect_following()
    data = batch_compare_following()
    findings(data)
    executionTime = (time() - startTime)
    print('Execution time in seconds: ' + str(executionTime))





