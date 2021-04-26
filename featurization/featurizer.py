""" Computes the full feature vectors based on the word2vec model, and subreddits
"""
import pymysql
from collections import defaultdict
import json
import numpy as np
import mysql
import pickle
from gensim.models import KeyedVectors
from torch.utils.data import Dataset

sub_n = 5000
# takes in a subreddit_arr and converts it to the normalized indexed form
# creates a sparse vector of subreddit activity
def normalized_subreddit_vector(subreddit_arr, sub_dict):
    v = np.zeros(sub_n)

    for subreddit in subreddit_arr:
        if(subreddit in sub_dict):
            v[sub_dict[subreddit]] +=1

    norm = np.linalg.norm(v)
    if norm == 0:
        return v

    return v / np.linalg.norm(v)

def tokenizer(arr_string):
    return json.loads(arr_string.replace("\'", "\""))

# converts each document to tokens and then returns a list of indices
def process_document(w2v, doc):
    indices = []
    for token in tokenizer(doc):
        if token in w2v.wv.key_to_index:
            indices.append(w2v.wv.key_to_index[token])
    
    return indices


# generate dictionary of subreddit -> index
def gen_sub_dict_worker(indices, cleaned_rows, tally_dict):
    for i in indices:  
        row = cleaned_rows[i]
        subreddit_arr = tokenizer(row['subreddit_arr'])
        for s in subreddit_arr:
            tally_dict[s] += 1
        if(i % 10000 == 0):
            print("Done processing %d rows" % i)
    print("Worker has completed range: %d-%d" % (indices[0], indices[-1]))


def split(a, n):
    k, m = divmod(len(a), n)
    return list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))

def gen_sub_dict():
    cleaned_rows = mysql.fetch_all_cleaned();
    print("Fetched cleaned rows")
    
    # count # of subreddits
    tally = defaultdict(int)
        
    # don't parallelize
    #n_workers = 16
    #parts = split(range(len(cleaned_rows)), n_workers)
    #with ProcessPoolExecutor(max_workers=n_workers) as e:
        #for i in range(n_workers):
         #   e.submit(gen_sub_dict_worker, parts[i], cleaned_rows, tally)

    gen_sub_dict_worker(range(len(cleaned_rows)), cleaned_rows, tally)
    print("Read %d unique subreddits" % len(tally))

    # sort most popular subreddits
    sorted_subs = sorted(tally.items(), key=lambda v: v[1])
    sorted_subs.reverse()
    print(sorted_subs)
    
    # keep next top 5000 subreddits
    sub_5k = sorted_subs[:sub_n]
    sub_dict = dict([(sub_5k[i][0], i) for i in range(len(sub_5k))])
    
    print("Kept a dict of the top %d subreddits" % (sub_n))
    pickle.dump(sub_dict, open("sub_dict.p", "wb"))
    return sub_dict
    
def load_data():
    # load w2v model
    w2v = KeyedVectors.load('embeddings/full.model')
    
    # fetch users and cleaned tables
    cleaned_rows = mysql.fetch_all_cleaned();
    users_rows = mysql.fetch_users();

    # map of user to (label, features) tuples
    data = dict()
    
    sub_dict = pickle.load(open("sub_dict.p", "rb"))
    
    # load username list
    usernames = []
    for r in users_rows:
        usernames.append(r['username'])
    
    
    for r in users_rows:        
        data[r['username']] = {'is_bot': r['is_bot'], 'karma': r['karma'], 'is_mod': r['is_mod']}
        
    count = 0
    for r in cleaned_rows:
        username = r['username']
        if username == '[deleted]':
            continue
            
        data[username]['document'] = process_document(w2v, r['document'])
        subreddits = tokenizer(r['subreddit_arr'])
        sub_v = normalized_subreddit_vector(subreddits, sub_dict)
        data[username]['subreddit_v'] = sub_v
        
        count+=1
        if(count % 1000 == 0):
            print("Processed %d entries" % count)
            
    return data
        
if __name__ == "__main__":
    gen_sub_dict()