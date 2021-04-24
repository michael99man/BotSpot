from collection import database
import json
from collection.retriever import Retriever


# fetch users from r/politics and state subreddits
# OBSOLETE, REWRITE INTO WORKERS IF NEEDED
def fetch_users(retriever):
    politics_users, states_users = pickler.read()
    # fetch 100k flaired users from r/politics
    print("Starting r/politics user scrape")
    updated_users = retriever.get_politics_users(100000, politics_users)
    print(updated_users)
    print("FETCHED %d users" % len(updated_users))

    # fetch users from each state subreddit
    retriever.get_state_subreddit_users(states_users)


def get_bot_set():
    file = open('collection/bots.json')
    data = json.load(file)
    file.close()

    bots = set()
    for entry in data["data"]:
        print(entry)
        # add bot to list of usernames
        bots.add(entry['bot']);

    return bots

# convert bots.json and insert into db
def write_bot_usernames():
    entries = []
    for bot in get_bot_set():
        entries.append((bot, 1))

    print("Adding %d bot usernames" % len(entries))
    database.insert_users(entries)

def write_normal_usernames():
    retriever = Retriever(0)
    normies = retriever.get_normal_users(10000)
    entries = []

    bots = get_bot_set()

    for normie in normies:
        if(normie in bots):
            print("Found %s in bots" % normie)
            continue

        entries.append((normie, 0))

    print(entries)
    print("Adding %d normie usernames" % len(entries))
    database.insert_users(entries);


def clean_low_and_null():
    rows = database.get_activity_counts()
    database.delete_low_count_users(rows)

# fetches and writes is mod and karma data
def write_user_information():
    retriever = Retriever(0)
    users = database.get_all_users()
    for row in users:
        username = row[0]
        if row[2] != 0:
            continue
        (is_mod, karma) = retriever.get_user_data(username)
        print("User %s: is_mod (%d), karma (%d)" % (username, is_mod, karma))
        database.write_mod_karma(username, is_mod, karma)


def main():
    write_user_information()

if __name__ == "__main__":
    main()
