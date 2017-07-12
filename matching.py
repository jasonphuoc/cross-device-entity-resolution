#Machine learning implementation for Big Data class

from __future__ import print_function
from sklearn import svm
from sklearn import linear_model
from sklearn.metrics import r2_score
from sklearn.metrics import f1_score
from sklearn.feature_selection import f_regression
from multiprocessing.dummy import Pool as ThreadPool
from sklearn.linear_model import SGDClassifier
from datetime import datetime
import pandas as pd
import numpy as np
import cPickle as pickle
import operator
import sys
        
models = ["least squares", "support vector regression", "stochastic gradient descent", "ridge", "lasso"]
features = ["words", "word_pairs", "word_trios", "word_quads", "words_ratio", "pairs_ratio", "trios_ratio", "quads_ratio", "access_intervals", "daily_activity", "hours_indep_overlap_count", "days_overlap", "days_overlap_count", "hours_overlap", "hours_overlap_count", "timerange", "weekly_activity", "url_l1", "url_l2", "url_l3", "url_l4", "url_l5"]
target = ["match"]

#usage: python matching.py 

def main():
    #parse data, shuffle and divide between validation and training sets
    data = parse_model_data()
    data = data.sample(frac=1)
    validation, train = np.split(data, [int(.2 * len(data))])

    #select optimal features
    best_features = test_features(validation, s_model=0)[0]
    
    #create model
    m1 = model(train, selected_features=best_features, selected_model=0)[0]

    #perform matching
    match(m1, best_features)

def match(model, selected_features):
    matching_matrix = parse_match_data()
    
    predictions = pd.DataFrame(model.predict(matching_matrix[selected_features])) 
    predictions.columns = ["prediction"]
    
    matching_matrix = matching_matrix.reset_index(drop=True)
    full_match_matrix = pd.concat([matching_matrix, predictions], axis=1)
    sorted_matrix = full_match_matrix.sort_values(["prediction"], ascending=0)
      
    print("All predictions:\n")    
    print(sorted_matrix)
    
    submission = sorted_matrix.iloc[:215307]
    with open('submission.txt','w') as output_file:
        for index, row in submission.iterrows(): 
            user1, user2 = row["users"].split()
            output_file.write("%s,%s\n" % (user1, user2))
        
    print(datetime.now().strftime("Done: %H:%M:%S"))

# if no parameters given for selected features, defaults to all

def model(model_matrix, selected_features=features, selected_model=0):    
    #divide modeling matrix into train and test sets (train: 80%, test: 20%)
    
    test, train = np.split(model_matrix, [int(.2 * len(model_matrix))])
    
    #build and test model
    
    model = None
    if selected_model == 0:
        model = linear_model.LinearRegression()
    elif selected_model == 1:
        model = svm.SVR(kernel='linear')
    elif selected_model == 2:
        model = SGDClassifier(loss="hinge", penalty="l2")
    elif selected_model == 3:
        model = linear_model.Ridge(alpha=.5)
    elif selected_model == 4:
        model = svm.SVR()
    else:
        model = linear_model.Lasso(alpha=0.1)
    
    model.fit(train[selected_features], train[target].values.ravel())
    predictions = pd.DataFrame(model.predict(test[selected_features]))
    predictions.columns = ["prediction"]

    #convert continuous predictions into discrete values to compute f1 score 
    
    adjusted_prediction = []
    for index, row in predictions.iterrows():
        if row["prediction"] >= 0:
            adjusted_prediction.append(1)
        else:
            adjusted_prediction.append(-1)
    rounded_prediction = pd.DataFrame({"adjusted_prediction": adjusted_prediction})

    test = test.reset_index(drop=True)
    full_results = pd.concat([test, predictions, rounded_prediction], axis=1)
    sorted_matrix = full_results.sort_values(["prediction"], ascending=0)
    
    f1 = f1_score(full_results[target], full_results[["adjusted_prediction"]])
    
    return [model, f1]

#chooses best features based on f1 score; 

def test_features(model_matrix, s_model=0):
    features = calc_f_values(model_matrix)
    
    best_features = []
    best_f1 = 0
    
    for x in range(1, len(features) + 1):
        f1 = model(model_matrix, selected_features=features[:x], selected_model=s_model)[1]
        print("Features tested:", features[:x])
        print("F1 score:", f1)
        
        if f1 > best_f1:
            best_f1 = f1
            best_features = features[:x]
    
    print("Best features:", best_features)
    return [best_features, best_f1]

def test_models(model_matrix):
    best_features = []
    best_model = 0
    best_f1 = 0
    
    for x in range(5):
        print("Testing model:", models[x])
        results = test_features(model_matrix, s_model=x)
        if best_f1 < results[1]:
            best_features = results[0]
            best_f1 = results[1]
            best_model = x
    
    print("Selected features:", best_features)
    print("Selected model:", models[x])
    return [best_features, best_model]

#sorts features based on f values

def calc_f_values(data):
    model_matrix = data
    f_vals_d = {}
    
    f_vals, p_vals = f_regression(model_matrix[features], model_matrix[target].values.ravel())
    
    #combine f_val with feature
    for x in range(len(f_vals)):
        f_vals_d[features[x]] = f_vals[x]
    
    #sort from best feature to worst
    sorted_f_vals = sorted(f_vals_d.items(), key=operator.itemgetter(1), reverse=True)
    sorted_features = []
    for item in sorted_f_vals:
        sorted_features.append(item[0])
    
    print("F values:", sorted_f_vals)
    return sorted_features

def parse_model_data():
    
    print(datetime.now().strftime("Parsing data start: %H:%M:%S"))
    
    modeling_words = pd.read_csv("./data/pairs_for_modeling_user_unique_words2.csv", header=None, names=["users", "words"])
    modeling_word_pairs = pd.read_csv("./data/pairs_for_modeling_user_word_pairs2.csv", header=None, names=["users", "word_pairs"])
    modeling_word_trios = pd.read_csv("./data/pairs_for_modeling_user_word_trios2.csv", header=None, names=["users", "word_trios"])
    modeling_word_quads = pd.read_csv("./data/pairs_for_modeling_user_word_quads2.csv", header=None, names=["users", "word_quads"])
    modeling_words_ratio = pd.read_csv("./data/pairs_for_modeling_ratios_user_unique_words2.csv", header=None, names=["users", "words_ratio"])
    modeling_word_pairs_ratio = pd.read_csv("./data/pairs_for_modeling_ratios_user_word_pairs2.csv", header=None, names=["users", "pairs_ratio"])
    modeling_word_trios_ratio = pd.read_csv("./data/pairs_for_modeling_ratios_user_word_trios2.csv", header=None, names=["users", "trios_ratio"])
    modeling_word_quads_ratio = pd.read_csv("./data/pairs_for_modeling_ratios_user_word_quads2.csv", header=None, names=["users", "quads_ratio"])
    modeling_pair_targets = pd.read_csv("./data/pairs_for_modeling_targets2.csv", header=None, names=["users", "match"])
    
    access_intervals = pd.read_csv("./data/pairs_for_modeling_access_intervals2.csv", header=None, names=["users", "access_intervals"])
    daily_activity = pd.read_csv("./data/pairs_for_modeling_daily_activity2.csv", header=None, names=["users", "daily_activity"])
    days_overlap = pd.read_csv("./data/pairs_for_modeling_days_overlap2.csv", header=None, names=["users", "days_overlap"])
    days_overlap_count = pd.read_csv("./data/pairs_for_modeling_days_overlap_count2.csv", header=None, names=["users", "days_overlap_count"])
    hours_overlap = pd.read_csv("./data/pairs_for_modeling_hours_overlap2.csv", header=None, names=["users", "hours_overlap"])
    hours_overlap_count = pd.read_csv("./data/pairs_for_modeling_hours_overlap_count2.csv", header=None, names=["users", "hours_overlap_count"])
    timerange = pd.read_csv("./data/pairs_for_modeling_timerange2.csv", header=None, names=["users", "timerange"])
    weekly_activity = pd.read_csv("./data/pairs_for_modeling_weekly_activity2.csv", header=None, names=["users", "weekly_activity"])
    hours_indep_overlap_count = pd.read_csv("./data/pairs_for_modeling_hours_indep_overlap_count2.csv", header=None, names=["users", "hours_indep_overlap_count"])
    
    url_l1 = pd.read_csv("./data/pairs_for_modeling2_url_l1.csv", header=None, names=["users", "url_l1"])
    url_l2 = pd.read_csv("./data/pairs_for_modeling2_url_l2.csv", header=None, names=["users", "url_l2"])
    url_l3 = pd.read_csv("./data/pairs_for_modeling2_url_l3.csv", header=None, names=["users", "url_l3"])
    url_l4 = pd.read_csv("./data/pairs_for_modeling2_url_l4.csv", header=None, names=["users", "url_l4"])
    url_l5 = pd.read_csv("./data/pairs_for_modeling2_url_l5.csv", header=None, names=["users", "url_l5"])
    
    model_matrix = modeling_pair_targets.merge(modeling_words, on="users").merge(modeling_word_pairs, on="users").merge(modeling_word_trios, on="users").merge(modeling_word_quads, on="users").merge(modeling_words_ratio, on="users").merge(modeling_word_pairs_ratio, on="users").merge(modeling_word_trios_ratio, on="users").merge(modeling_word_quads_ratio, on="users").merge(access_intervals, on="users").merge(daily_activity, on="users").merge(days_overlap, on="users").merge(days_overlap_count, on="users").merge(hours_overlap, on="users").merge(hours_overlap_count, on="users").merge(hours_indep_overlap_count, on="users").merge(timerange, on="users").merge(weekly_activity, on="users").merge(url_l1, on="users").merge(url_l2, on="users").merge(url_l3, on="users").merge(url_l4, on="users").merge(url_l5, on="users")
    
    print(datetime.now().strftime("Parsing data end: %H:%M:%S"))
    
    return model_matrix

def parse_match_data():
    print(datetime.now().strftime("Starting match parsing: %H:%M:%S"))
    
    #assemble matrix for matching 

    words = pd.read_csv("./data/pairs_for_matching_common_words.csv", header=None, names=["users", "words"])
    word_pairs = pd.read_csv("./data/pairs_for_matching_user_word_pairs.csv", header=None, names=["users", "word_pairs"])
    word_trios = pd.read_csv("./data/pairs_for_matching_user_word_trios.csv", header=None, names=["users", "word_trios"])
    word_quads = pd.read_csv("./data/pairs_for_matching_user_word_quads.csv", header=None, names=["users", "word_quads"])
    words_ratio = pd.read_csv("./data/pairs_for_matching_ratios_user_unique_words.csv", header=None, names=["users", "words_ratio"])
    pairs_ratio = pd.read_csv("./data/pairs_for_matching_ratios_user_word_pairs.csv", header=None, names=["users", "pairs_ratio"])
    trios_ratio = pd.read_csv("./data/pairs_for_matching_ratios_user_word_trios.csv", header=None, names=["users", "trios_ratio"])
    quads_ratio = pd.read_csv("./data/pairs_for_matching_ratios_user_word_quads.csv", header=None, names=["users", "quads_ratio"])
    access_intervals = pd.read_csv("./data/pairs_for_matching_access_intervals.csv", header=None, names=["users", "access_intervals"])
    daily_activity = pd.read_csv("./data/pairs_for_matching_daily_activity.csv", header=None, names=["users", "daily_activity"])
    days_overlap = pd.read_csv("./data/pairs_for_matching_days_overlap.csv", header=None, names=["users", "days_overlap"])
    days_overlap_count = pd.read_csv("./data/pairs_for_matching_days_overlap_count.csv", header=None, names=["users", "days_overlap_count"])
    hours_overlap = pd.read_csv("./data/pairs_for_matching_hours_overlap.csv", header=None, names=["users", "hours_overlap"])
    hours_overlap_count = pd.read_csv("./data/pairs_for_matching_hours_overlap_count.csv", header=None, names=["users", "hours_overlap_count"])
    timerange = pd.read_csv("./data/pairs_for_matching_timerange.csv", header=None, names=["users", "timerange"])
    weekly_activity = pd.read_csv("./data/pairs_for_matching_weekly_activity.csv", header=None, names=["users", "weekly_activity"])
    hours_indep_overlap_count = pd.read_csv("./data/pairs_for_matching_hours_indep_overlap_count.csv", header=None, names=["users", "hours_indep_overlap_count"])

    url_l1 = pd.read_csv("./data/pairs_for_matching_url_l1.csv", header=None, names=["users", "url_l1"])
    url_l2 = pd.read_csv("./data/pairs_for_matching_url_l2.csv", header=None, names=["users", "url_l2"])
    url_l3 = pd.read_csv("./data/pairs_for_matching_url_l3.csv", header=None, names=["users", "url_l3"])
    url_l4 = pd.read_csv("./data/pairs_for_matching_url_l4.csv", header=None, names=["users", "url_l4"])
    url_l5 = pd.read_csv("./data/pairs_for_matching_url_l5.csv", header=None, names=["users", "url_l5"])
    
    matching_matrix = words.merge(word_pairs, on="users").merge(word_trios, on="users").merge(word_quads, on="users").merge(words_ratio, on="users").merge(pairs_ratio, on="users").merge(trios_ratio, on="users").merge(quads_ratio, on="users").merge(access_intervals, on="users").merge(daily_activity, on="users").merge(days_overlap, on="users").merge(days_overlap_count, on="users").merge(hours_overlap, on="users").merge(hours_overlap_count, on="users").merge(hours_indep_overlap_count, on="users").merge(timerange, on="users").merge(weekly_activity, on="users").merge(url_l1, on="users").merge(url_l2, on="users").merge(url_l3, on="users").merge(url_l4).merge(url_l5, on="users")
    
    print(datetime.now().strftime("Done match parsing: %H:%M:%S"))
    return matching_matrix

if __name__ == "__main__":
	main()