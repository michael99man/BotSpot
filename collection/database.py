#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Handles interfacing with Sqlite3 database
"""

import sqlite3
conn = sqlite3.connect('data.nosync/reddit_data.db')

def insert_users(entries):
    c = conn.cursor()
    c.executemany('INSERT INTO users (username, is_bot, karma) VALUES (?, ?, 0)', entries)
    for row in c.execute('SELECT * FROM users'):
        print(row)
    conn.commit()

# load the contents of the users table into memory
def get_all_users():
    c = conn.cursor()
    c.execute('SELECT * FROM users')
    rows = c.fetchall()

    print("Loaded %d users from DB" % len(rows))
    return rows

def insert_activity_entries(entries):
    c = conn.cursor()
    c.executemany('INSERT INTO activity (username, timestamp, subreddit, content, is_post) VALUES (?, ?, ?,?,?)', entries)
    conn.commit()

# get the set of users that have activity rows associated with them (i.e. done)
def get_completed_users():
    c = conn.cursor()
    c.execute('SELECT DISTINCT username FROM activity')
    rows = c.fetchall()

    completed_users = set()
    for row in rows:
        completed_users.update([row[0]])

    print("%d users have been already processed" % len(completed_users))
    return completed_users


# delete all users that don't have activity associated with them (because of errors, privacy settings)
def delete_null_users():
    all_users = get_all_users()
    completed_users = get_completed_users()

    null_users = []

    for row in all_users:
        username = row[0]
        if username not in completed_users:
            null_users.append(username)

    null_users_str = str(null_users)
    null_users_str = null_users_str.replace("[", "(").replace("]", ")")
    print("Deleting %d null users: %s" % (len(null_users), null_users_str))

    c = conn.cursor()
    c.execute('DELETE FROM users where username in ' + null_users_str)
    conn.commit()


# delete users that have activity counts outside of the range [500,3000]
def delete_low_count_users(rows):
    bad_users = []
    for row in rows:
        if row[1] < 200:
            bad_users.append(row[0])

    bad_users_str = str(bad_users).replace("[", "(").replace("]", ")")
    print("Deleting %d users" % len(bad_users))
    print(bad_users_str)

    c = conn.cursor()
    c.execute('DELETE FROM users where username in ' + bad_users_str)
    c.execute('DELETE FROM activity where username in ' + bad_users_str)
    conn.commit()



# gets the distinct users with their activity counts
def get_activity_counts():
    c = conn.cursor()
    c.execute("SELECT username, COUNT(*) FROM activity GROUP BY username")
    rows = c.fetchall()
    print(rows)
    return rows

# gets all activity entries associated with a username
def get_activity_for_user(username):
    c = conn.cursor()
    c.execute("SELECT * FROM activity WHERE username='" + username + "'")
    rows = c.fetchall()
    print("Fetched %d rows for %s" % (len(rows), username))
    return rows

# updates users table with karma and mod status
def write_mod_karma(username, is_mod, karma):
    c = conn.cursor()
    c.execute("UPDATE users SET is_mod=?, karma=? WHERE username=?", (is_mod, karma, username))
    conn.commit()