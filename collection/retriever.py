#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Helper functions to interact with the Reddit API through PRAW and PSAW
"""

import praw
from praw.models import MoreComments
from psaw import PushshiftAPI

ids = ["pw9MViKdnvWhvg", "NS3agxMZ4b-jsg", "9LMNOYIf3-aHcg", "25ziqrlE-oLLaw", "Z9o5Pqo2wb3DOQ", "eimkyYu2Ilur0Q", "0Xr2T0wag5tpzg", "qyxztQqeBQS5sA", "roBbLMyO9IePSA", "RvUqLB0PyFPeTw", "_f_atr1DIqcnNw","Q1Oy8MK_h-3FYg", "qXHeySUMAL4zzw", "2JfJq5rziYDE0w","gerQxFBoNoECjA","VaU5LAmEjCXtnw","EbAxC0O6FlZZ6w","cKlKIpDcc6SWjQ","40aYBMCM1KxqQw", "y4b76WQNiDa6Lg"]
secrets = ["N3DpHmI8cRJ1vUZQmCyjrj3_KSAcZg", "4A-2iK0DNfsRo0ZuYpMxtkAqRQ9rGQ", "zn84HS4knQZxJqQq0lAkSnvXCwT0vw", "K_FN3Hh8BDthyFtCjU6x1ITX9inXLQ", "jUEDw8bLgqAbu3eZcXaCamdnYvmFRQ", "vJZyiZ0KDWoM-uCvOLrlT6fUL4SBfg", "isOyyTg_Z0gWaVPb78FUcJKZlamvbg", "GUpfdN9DWjYp_0ri6soT_gU9fwGOKA", "CGh8Ue8MgHkwnw8YyeDEcmz2SZ3itQ", "gOvnjiUThZXK9GaZyfJofFEwaLo2bg", "nvH5jgeLxj2k10-LdIphUSrVzCvV4w","ErDsETn7CwOzmCaRShdz3PL9EkDwHA","tL661goKFwcy0_t6hqBj47pvJjJ0GQ","1J1ICzRp8Tqqu2wyZoX_dLfzXsQiPg","PZOwoIo183R0XuDVBXWwSTSDdKOYKQ","A3Q7tQ3aNXvj_bfL-epYKFVxL_schw","jqXe2JJ3QEw6yvsbLx1h-msl0XbPdg","kalud-IZs6mqFg6hbquc4yKW_zoISQ","7js4pFgg9qaWBMloMkATKzFGiGvyNw", "GGRJLEtCcsTgl0vBqSJH1OfwlvJJhg"]
usernames = ["ele574-scraper", "ele574-scraper1", "ele574-scraper2", "ele574-scraper3", "ele574-scraper4", "ele574-scraper5", "ele574-scraper6", "ele574-scraper7", "ele574-scraper8", "ele574-scraper9", "ele574-scraper10", "ele574-scraper11", "ele574-scraper12", "ele574-scraper13", "ele574-scraper14", "ele574-scraper15", "ele574-scraper16", "ele574-scraper17", "ele574-scraper18", "ele574-scraper19"]

class Retriever:
    def __init__(self, instance):
        self.reddit = praw.Reddit(
            client_id=ids[instance],
            client_secret=secrets[instance],
            password="correct horse battery staple",
            user_agent="script:net.michaelman:v0.1 (by u/ele574-scraper)",
            username= usernames[instance],
        )
        self.instance = instance
        self.pushshift = PushshiftAPI()

    # fetch users from r/askreddit
    def get_normal_users(self, n_users):
        users = set()

        end_epoch = 1619227982
        # keep looping through entries
        while True:
            entries = list(self.pushshift.search_comments(before=end_epoch,
                                                          subreddit='askreddit',
                                                          filter=['author'],
                                                          limit=1000000))
            print("Fetched %d entries before %d" % (len(entries), end_epoch))
            for e in entries:
                # the created epoch should be decreasing on each entry
                end_epoch = e.created_utc
                username = e.author

                # heuristic to ignore obvious bots
                if(username.find("bot") != -1):
                    print("Ignoring: %s" % username)
                    continue

                if (username not in users):
                    users.add(username)
                    print("%d | %s" % (len(users), username))
                    if (len(users) == n_users):
                        return users


    def get_user_data(self, username):
        print(username)
        redditor = self.reddit.redditor(username)


        if(username == "IamKatieBorg" or username=="[deleted]"):
            return (-1, -1)

        if(hasattr(redditor, "is_mod")):
            is_mod = redditor.is_mod
        else:
            return (-1, -1)
        # print("%s is mod: %s" % (username, str(is_mod)))

        karma = redditor.comment_karma + redditor.link_karma
        # print("%s has karma: %d" % (username, karma))

        return (int(is_mod == True), karma)


    # returns activity as a list of entries from Reddit API
    # each entry will be of the form (username, subreddit, is_post (boolean), timestamp, content)
    def get_activity_history(self, username):
        entries = []

        # fetch all posts from this user
        submissions = self.reddit.redditor(username).submissions.top("all", limit=None)

        for submission in submissions:
            if submission.selftext != "":
                content = submission.selftext
            else:
                # fallback to title if no body
                content = submission.title
            timestamp = round(submission.created_utc)

            entry = (username, timestamp, submission.subreddit.display_name, content, 1)
            entries.append(entry)

        # fetch all comments from this user
        comments = self.reddit.redditor(username).comments.top("all", limit=None)
        comment_ids = set()
        count = 0
        for comment in comments:
            content = comment.body
            timestamp = round(comment.created_utc)

            entry = (username, timestamp, comment.subreddit.display_name, content, 0)
            entries.append(entry)
            comment_ids.update([comment.id])
            count += 1

        if count == 1000:
            # fetch most controversial comments to increase total comment count
            comments = self.reddit.redditor(username).comments.controversial("all", limit=None)
            for comment in comments:
                if comment.id in comment_ids:
                    continue
                content = comment.body
                timestamp = round(comment.created_utc)

                entry = (username, timestamp, comment.subreddit.display_name, content, 0)
                entries.append(entry)
                comment_ids.update([comment.id])

        return entries
