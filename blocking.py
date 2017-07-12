# This file builds on the baseline solution code provided by CIKM, which is found here: https://drive.google.com/drive/u/0/folders/0B7XZSACQf0KdNXVIUXEyVGlBZnc 

import json
import pickle
import numpy as np
from tqdm import tqdm
from datetime import datetime
import pandas as pd
import itertools
from sklearn.neighbors import KNeighborsClassifier
from multiprocessing.dummy import Pool as ThreadPool

users = pickle.load(open("../data-train-dca/users.p", "rb"))
tf_test = pickle.load(open("../data-train-dca/tf_test.p", "rb"))
tf = pickle.load(open("../data-train-dca/tf.p", "rb"))

print("Done with loading pickles")

knn = KNeighborsClassifier(n_neighbors=1)
knn.fit(tf_test, range(1, tf_test.shape[0] + 1))
pool = ThreadPool(16)

def get_predict(line):
    res = []
    user_id, tokens = line.strip().split('\t')
    cur_vect = tf.transform([tokens])
    tmp = knn.kneighbors(X=tf.transform([tokens]), n_neighbors=15, return_distance=True)
    for i in range(len(tmp[0][0])):
        if 0 < tmp[0][0][i] < 0.9999:
            if user_id < users[tmp[1][0][i]]:
                res.append((user_id, users[tmp[1][0][i]], tmp[0][0][i]))
            else:
                res.append((users[tmp[1][0][i]], user_id, tmp[0][0][i]))
    return res

lines = open('../data-train-dca/facts_test.tsv').readlines()
results = pool.map(get_predict, lines)
q = sorted(list(set([y for x in results for y in x])), key=lambda x: x[-1])
print("Done with knn")

pickle.dump(q, open("../data-train-dca/q.p", "wb"))
        
with open('../data-train-dca/pairs_for_matching.csv','w') as f_out:
    for x in tqdm(q):
        f_out.write("%s,%s\n" % (x[0], x[1]))