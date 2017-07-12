import cPickle as pickle

def main():
    get_unique_titles()
    
    print("starting words")
    get_common_word_groups("user_unique_words")
    print("word pairs")
    get_common_word_groups("user_word_pairs")
    print("word trios")
    get_common_word_groups("user_word_trios")
    print("word quads")
    get_common_word_groups("user_word_quads")
    
    print("starting words ratio")
    get_common_groups_ratio("user_unique_words")
    print("word pairs")
    get_common_groups_ratio("user_word_pairs")
    print("word trios")
    get_common_groups_ratio("user_word_trios")
    print("word quads")
    get_common_groups_ratio("user_word_quads")

def get_unique_titles():
    user_titles = pickle.load(open("../data-train-dca/user_titles.p", "rb"))
    user_unique_titles = {}
    for user, titles in user_titles.iteritems():
        user_unique_titles[user] = set(titles)
    pickle.dump(user_unique_titles, open("../data-train-dca/user_unique_titles.p", "wb"))    

def get_word_groups():
    user_unique_titles = pickle.load(open("../data-train-dca/user_unique_titles.p", "rb"))
    user_word_singles = {}
    user_word_pairs = {}
    user_word_trios = {}
    user_word_quads = {}
    
    for user, titles in user_unique_titles.iteritems():
        words_singles = set()
        word_pairs = set()
        word_trios = set()
        word_quads = set()
        for title in titles:
            words = title.split()
            words_singles.add(words)
            if len(words) > 1:
                pairs = [words[i:i+2] for i in xrange(len(words)-1)]
                for pair in pairs:
                    word_pairs.add(pair[0] + " " + pair[1])
            if len(words) > 2:
                trios = [words[i:i+3] for i in xrange(len(words)-2)]
                for trio in trios:
                    word_trios.add(trio[0] + " " + trio[1] + " " + trio[2])
            if len(words) > 3:
                quads = [words[i:i+4] for i in xrange(len(words)-3)]
                for quad in quads:
                    word_quads.add(quad[0] + " " + quad[1] + " " + quad[2] + " " + quad[3])        
        user_word_singles[user] = word_singles
        user_word_pairs[user] = word_pairs
        user_word_trios[user] = word_trios
        user_word_quads[user] = word_quads
    
    print("starting word singles pickle")
    pickle.dump(user_word_singles, open("../data-train-dca/user_unique_words.p", "wb")) 
    
    print("starting word pairs pickle")
    pickle.dump(user_word_pairs, open("../data-train-dca/user_word_pairs.p", "wb")) 
    
    print("starting word trios pickle")
    pickle.dump(user_word_trios, open("../data-train-dca/user_word_trios.p", "wb")) 
    
    print("starting word quads pickle")
    pickle.dump(user_word_quads, open("../data-train-dca/user_word_quads.p", "wb")) 
    
def get_common_word_groups(file_name):
    dic = pickle.load(open("../data-train-dca/%s.p" % file_name, "rb"))
    print("Done unpickling")
    
    with open("../data-train-dca/pairs_for_matching_%s.csv" % file_name,'w') as output_file:
        with open('../data-train-dca/pairs_for_matching.csv') as input_file:
            for line in input_file:
                user1, user2 = line.strip().split(",")
                common = len(dic[user1].intersection(dic[user2]))
                output_file.write("%s %s,%s\n" % (user1, user2, common))
    with open("../data-train-dca/pairs_for_modeling_%s2.csv" % file_name,'w') as output_file:
        with open('../data-train-dca/pairs_for_modeling2.csv') as input_file:
            for line in input_file:
                user1, user2, match = line.strip().split(",")
                common = len(dic[user1].intersection(dic[user2]))
                output_file.write("%s %s,%s\n" % (user1, user2, common))

def get_common_groups_ratio(file_name):
    dic = pickle.load(open("../data-train-dca/%s.p" % file_name, "rb"))
    print("Done unpickling")
    
    with open("../data-train-dca/pairs_for_matching_ratios_%s.csv" % file_name,'w') as output_file:
        with open('../data-train-dca/pairs_for_matching.csv') as input_file:
            for line in input_file:
                user1, user2 = line.strip().split(",")
                common = float(len(dic[user1].intersection(dic[user2])))
                total = float(len(dic[user1].union(dic[user2])))
                ratio = 0
                if total != 0:
                    ratio = common / total
                output_file.write("%s %s,%s\n" % (user1, user2, ratio))
    
    with open("../data-train-dca/pairs_for_modeling_ratios_%s2.csv" % file_name,'w') as output_file:
        with open('../data-train-dca/pairs_for_modeling2.csv') as input_file:
            for line in input_file:
                user1, user2, match = line.strip().split(",")
                common = float(len(dic[user1].intersection(dic[user2])))
                total = float(len(dic[user1].union(dic[user2])))
                ratio = 0
                if total != 0:
                    ratio = common / total
                output_file.write("%s %s,%s\n" % (user1, user2, ratio))
                
if __name__ == "__main__":
	main()