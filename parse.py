# This file builds on the baseline solution code provided by CIKM, which is found here: https://drive.google.com/drive/u/0/folders/0B7XZSACQf0KdNXVIUXEyVGlBZnc 

import json
import cPickle as pickle
import numpy as np
from tqdm import tqdm
from sklearn.feature_extraction.text import TfidfVectorizer

def main():
    parse_domains()
    parse_titles()
    parse_logs_domains()
    parse_logs_titles()
    gen_predict_users()
    gen_tf_idf()
    
def parse_domains():
    urls_tokens = {}
    with open('../data-train-dca/urls.csv') as f_in:
        for line in tqdm(f_in):
            key, tokens = line.strip().split(',')
            urls_tokens[key] = tokens.split('/')[0]
    pickle.dump(urls_tokens, open("../data-train-dca/url_domains.p", "wb"))

def parse_titles():    
    url_titles = {}
    with open("../data-train-dca/titles.csv") as input_data:
        for line in tqdm(input_data):
            key, value = line.strip().split(",")
            url_titles[key] = value
    pickle.dump(url_titles, open("../data-train-dca/url_titles.p", "wb")) 

    
#Must run parse_domains once before running this
def parse_logs_domains():
    facts = {}
    urls_tokens = pickle.load(open("../data-train-dca/url_domains.p", "rb"))
    with open('../data-train-dca/facts.json') as f_in:
        for line in tqdm(f_in):
            j = json.loads(line.strip())
            facts[j.get('uid')] = " ".join([urls_tokens[str(x['fid'])] for x in j.get('facts')])
    pickle.dump(facts, open("../data-train-dca/user_logs.p", "wb"))

#must run parse_titles once before running this
def parse_logs_titles():
    url_titles = pickle.load(open("../data-train-dca/url_titles.p", "rb"))
    user_titles = {}
    with open('../data-train-dca/facts.json') as f_in:
        for line in tqdm(f_in):
            j = json.loads(line.strip())
            titles = []
            for fact in j.get("facts"):
                if str(fact["fid"]) in url_titles:
                    titles.append(url_titles[str(fact["fid"])])
            user_titles[j.get("uid")] = titles
    pickle.dump(user_titles, open("../data-train-dca/user_titles.p", "wb"))
    
#Must run parse_logs() once before running this    
def gen_predict_users():    
    users_in_train = set()
    facts = pickle.load(open("../data-train-dca/user_logs.p", "rb"))
    with open('../data-train-dca/facts.txt','w') as f_out:
        with open('../data-train-dca/train.csv') as f_in:
            for line in tqdm(f_in):
                user1,user2 = line.strip().split(',')
                f_out.write("%s %s\n" % (facts[user1], facts[user2]))
                users_in_train.update([user1,user2])
    users_for_predict = set(facts.keys()).difference(users_in_train)
    users_for_predict_list = sorted(list(users_for_predict))
    pickle.dump(users_for_predict_list, open("../data-train-dca/users_predict.p", "wb"))
    with open('../data-train-dca/facts_test2.tsv','w') as f_out:
        for x in users_for_predict_list:
            f_out.write("%s\t%s\n" % (x, facts[x]))

#Must run gen_predict_users() once before running this            
def gen_tf_idf():
    users = pickle.load(open("../data-train-dca/users_predict.p", "rb"))
    tf = TfidfVectorizer(lowercase=False, preprocessor=None).fit(map(lambda x: x.strip().split('\t')[-1], open('../data-train-dca/facts.txt').readlines()))

    pickle.dump(tf, open("../data-train-dca/tf.p", "wb"))

    users, row_text = [], []
    with open('../data-train-dca/facts_test.tsv') as f_in:
        for line in tqdm(f_in):
            user, t = line.strip().split('\t')
            users.append(user)
            row_text.append(t)
    tf_test = tf.transform(row_text)
    pickle.dump(users, open("../data-train-dca/users.p", "wb"))
    pickle.dump(tf_test, open("../data-train-dca/tf_test.p", "wb"))
    
if __name__ == "__main__":
	main()