import json
import pickle
import numpy as np
from tqdm import tqdm
from collections import defaultdict

urls_tokens = {}
with open('../data-train-dca/urls.csv') as f_in:
    for line in tqdm(f_in):
        key, tokens = line.strip().split(',')
        urls_tokens[key] = tokens.split('/')[0]

#facts = {}
facts = defaultdict(list)
with open('../data-train-dca/facts.json') as f_in:
    for line in tqdm(f_in):
        j = json.loads(line.strip())
        y = [(str(x['ts']), urls_tokens[str(x['fid'])]) for x in j.get('facts')]
        facts.setdefault(j.get('uid'), y)
        
pickle.dump(facts, open("../data-train-dca/time_logs.p", "wb"))
