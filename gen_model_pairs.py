import cPickle as pickle
from tqdm import tqdm
import random
import csv

facts = pickle.load(open("../data-train-dca/user_logs.p", "rb"))

def main():
    gen_neg_matches()
    gen_pos_matches()
    
    #after this program, generate pairs_for_modeling2.csv by performing "cat neg_matches.csv pos_matches.csv > pairs_for_modeling2.csv" in terminal window
    
#randomly choose matching pairs from train.csv equal to the number of negative patches                 
def gen_pos_matches():
    with open('../data-train-dca/pos_matches.csv','w') as output:
        with open('../data-train-dca/train.csv', 'rb') as input_data:
            reader = csv.reader(input_data)
            train_users = list(reader)
            random.shuffle(train_users)
            model_set = train_users[:100000]
            for pair in model_set:
                output.write("%s,%s,%s\n" % (pair[0], pair[1], 1))

#randomly chooses two users among all users as negative matches                
def gen_neg_matches():
    with open('../data-train-dca/neg_matches.csv','w') as output:
        all_users = facts.keys()
        pairs = []
        for i in tqdm(range(1, 100000)):
            rand_index = random.randint(0, len(all_users) - 1)
            rand_index2 = random.randint(0, len(all_users) - 1)
            if all_users[rand_index] != all_users[rand_index2] and [all_users[rand_index], all_users[rand_index2]] not in pairs:
                output.write("%s,%s,%s\n" % (all_users[rand_index], all_users[rand_index2],-1))
                pairs.append([all_users[rand_index], all_users[rand_index2]])
            else:
                print "Duplicate pair"

def reformat_csv():
    with open("../data-train-dca/pairs_for_modeling_targets2.csv", 'w') as output_file:
        with open('../data-train-dca/pairs_for_modeling2.csv') as input_file:
            for line in input_file:
                user1, user2, match = line.strip().split(",")
                output_file.write("%s %s,%s\n" % (user1, user2, match))
                
if __name__ == "__main__":
	main()