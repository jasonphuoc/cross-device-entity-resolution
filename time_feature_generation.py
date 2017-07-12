import json
import sys
import pickle
import numpy as np
from tqdm import tqdm
from datetime import datetime
from collections import defaultdict
import traceback
import operator

def generate_activity_count():
    # uid -> list of tuples (timestamps , url domain) 
    facts = pickle.load(open("../data-train-dca/time_logs.p", "rb"))
    print("Done with loading facts")
    # for each uid, we want to store the count of activity each day of the week
    # count of activity each hour of each day
    weekly_activity_count = {} #(uid, day) -> activity count (weekly pattern) for each uid you have 7 buckets
    daily_activity_count = {} #(uid, day, hour)-> activity count (daily pattern) for each uid you have 24 * 7 buckets

    weekly_days_count = {} # counts number of mondays active, number of tuesday active, etc
    daily_hours_count = {} # counts number of mondays 1pm active, number of mondays 2pm active, etc

    temp = {} # stores existance of uid+year+month+day
    temp2 = {} # stores existence of uid+year+month+day+hour
    for uid, event_list in facts.items():
        for event in event_list:
            ts, url = event
            try:
                time_len = len(ts)
                time = datetime.utcfromtimestamp(int(ts[0:(10-time_len)])) # need to shave off last 3 digits because python datetime uses seconds not milliseconds
            except:
                print(ts)
                print(traceback.format_exc())

            # extract year, month, day and hour
            year = '%d' % time.year
            day = '%02d' % time.day
            month = '%02d' % time.month
            hour = '%02d' % time.hour
            # 0 - Mon, 6 - Sun
            day_of_week = time.weekday()

            # keys
            k = uid+str(year)+str(month)+str(day)
            k2 = uid+str(year)+str(month)+str(day)+str(hour)
            
            key = uid+str(day_of_week)
            key2 = uid+str(day_of_week)+str(hour)
            if k not in temp:
                temp[k] = 1
                if key in weekly_days_count:
                    weekly_days_count[key] += 1
                else:
                    weekly_days_count[key] = 1
            if k2 not in temp2:
                temp2[k2] = 1
                if key2 in daily_hours_count:
                    daily_hours_count[key2] += 1
                else:
                    daily_hours_count[key2] = 1
                

            if (key) in weekly_activity_count:
                weekly_activity_count[key] += 1
            else:
                weekly_activity_count[key] = 1

            if (key2) in daily_activity_count:
                daily_activity_count[key2] += 1
            else:
                daily_activity_count[key2] = 1
                
    
    pickle.dump(weekly_activity_count, open("../data-train-dca/weekly_activity_count.p", "wb"))
    pickle.dump(daily_activity_count, open("../data-train-dca/daily_activity_count.p", "wb"))
    
    pickle.dump(weekly_days_count, open("../data-train-dca/weekly_days_count.p", "wb"))
    pickle.dump(daily_hours_count, open("../data-train-dca/daily_hours_count.p", "wb"))

def generate_active_count():
    # uid -> list of tuples (timestamps , url domain) 
    facts = pickle.load(open("../data-train-dca/time_logs.p", "rb"))
    print("Done with loading facts")
    days_active = defaultdict(set) #(uid) -> [days] where days is in the format: 20170102 for jan 2 '17
    hours_active = defaultdict(set) # same as above but for hours
    hours_active_indep = defaultdict(set) # hours active independent of day or month

    days_count = {}
    hours_count = {}

    for uid, event_list in facts.items():
        isfirst = True
        previous = datetime.now()
        difference = 0
        event_list.sort(key = operator.itemgetter(1))

        days_active[uid] = set()
        hours_active[uid] = set()
        hours_active_indep[uid] = set()

        days_count[uid] = {}
        hours_count[uid] = {}
        
        for event in event_list:
            ts, url = event
            try:
                time_len = len(ts)
                time = datetime.utcfromtimestamp(int(ts[0:(10-time_len)])) # need to shave off last 3 digits because python datetime uses seconds not milliseconds
            except:
                print(ts)
                print(traceback.format_exc())
            year = '%d' % time.year
            day = '%02d' % time.day
            month = '%02d' % time.month
            hour = '%02d' % time.hour

            active_day = year+month+day
            active_hour = year+month+day+hour

            days_active[uid].add(active_day)
            hours_active[uid].add(active_hour)
            hours_active_indep[uid].add(hour)

            if active_day in days_count[uid]:
                days_count[uid][active_day] += 1
            else:
                days_count[uid][active_day] = 0

            if active_hour in hours_count[uid]:
                hours_count[uid][active_hour] += 1
            else:
                hours_count[uid][active_hour] = 0
                   
            
    pickle.dump(days_active, open("../data-train-dca/days_active.p", "wb"))
    pickle.dump(hours_active, open("../data-train-dca/hours_active.p", "wb"))
    pickle.dump(hours_active_indep, open("../data-train-dca/hours_active_indep.p", "wb"))
                
    pickle.dump(days_count, open("../data-train-dca/days_count.p", "wb"))
    pickle.dump(hours_count, open("../data-train-dca/hours_count.p", "wb"))

def generate_interval_timerange():
    # uid -> list of tuples (timestamps , url domain) 
    facts = pickle.load(open("../data-train-dca/time_logs.p", "rb"))
    print("Done with loading facts")
    access_intervals = {} #(uid) -> average time between accesses (need to sort activities by ts)
    timerange = {} #(uid) -> max ts - min ts

    maxts = 0
    mints = 0
    
    for uid, event_list in facts.items():
        isfirst = True
        previous = datetime.now()
        difference = 0.0
        event_list.sort(key = operator.itemgetter(1))

        for event in event_list:
            ts, url = event
            try:
                time_len = len(ts)
                time = datetime.utcfromtimestamp(int(ts[0:(10-time_len)])) # need to shave off last 3 digits because python datetime uses seconds not milliseconds
            except:
                print(ts)
                print(traceback.format_exc())

            if not isfirst:
                difference += ((time - previous).total_seconds())**2
                previous = time
            else:
                previous = time
                mints = time
                isfirst = False
            maxts = time
            
        difference = (np.sqrt(difference))/len(event_list)
        access_intervals[uid] = difference
        
        timerange[uid] = (maxts - mints).total_seconds()
        
    pickle.dump(timerange, open("../data-train-dca/timerange.p", "wb"))
    pickle.dump(access_intervals, open("../data-train-dca/access_intervals.p", "wb"))

def generate_activity_pair_features():
    weekly_activity_count = pickle.load(open("../data-train-dca/weekly_activity_count.p", "rb"))
    daily_activity_count = pickle.load(open("../data-train-dca/daily_activity_count.p", "rb"))
    weekly_days_count = pickle.load(open("../data-train-dca/weekly_days_count.p", "rb"))
    daily_hours_count = pickle.load(open("../data-train-dca/daily_hours_count.p", "rb"))

    # weekly activity
    with open('../data/pairs_for_matching_weekly_activity.csv','w') as output_file:
        with open('../data/pairs_for_matching.csv') as input_file:
            err = 0.0
            for line in input_file:
                user1, user2 = line.strip().split(",")
                for i in range(0,7):
                    user1_cnt = 0
                    user2_cnt = 0
                    user1_total_days = 0
                    user2_total_days = 0
                    x = user1+str(i)
                    y = user2+str(i)
                    if x in weekly_activity_count:
                        user1_cnt = weekly_activity_count[x]
                        user1_total_days = weekly_days_count[x]
                    if y in weekly_activity_count:
                        user2_cnt = weekly_activity_count[y]
                        user2_total_days = weekly_days_count[y]

                    if not user1_total_days == 0 and not user2_total_days == 0:
                        err += ((user1_cnt/user1_total_days) - (user2_cnt/user2_total_days))**2
                    elif user1_total_days == 0 and not user2_total_days == 0:
                        err += (user2_cnt/user2_total_days)**2
                    elif user2_total_days == 0 and not user1_total_days == 0:
                        err += (user1_cnt/user1_total_days)**2
                err = np.sqrt(err)
                output_file.write("%s %s,%d\n" % (user1, user2, err))
    with open('../data/pairs_for_modeling_weekly_activity2.csv','w') as output_file:
        with open('../data/pairs_for_modeling2.csv') as input_file:
            err = 0.0
            for line in input_file:
                user1, user2, match = line.strip().split(",")
                for i in range(0,7):
                    user1_cnt = 0
                    user2_cnt = 0
                    user1_total_days = 0
                    user2_total_days = 0
                    x = user1+str(i)
                    y = user2+str(i)
                    if x in weekly_activity_count:
                        user1_cnt = weekly_activity_count[x]
                        user1_total_days = weekly_days_count[x]
                    if y in weekly_activity_count:
                        user2_cnt = weekly_activity_count[y]
                        user2_total_days = weekly_days_count[y]

                    if not user1_total_days == 0 and not user2_total_days == 0:
                        err += ((user1_cnt/user1_total_days) - (user2_cnt/user2_total_days))**2
                    elif user1_total_days == 0 and not user2_total_days == 0:
                        err += (user2_cnt/user2_total_days)**2
                    elif user2_total_days == 0 and not user1_total_days == 0:
                        err += (user1_cnt/user1_total_days)**2
                        
                err = np.sqrt(err)
                output_file.write("%s %s,%d\n" % (user1, user2, err))

    # daily activity
    with open('../data/pairs_for_matching_daily_activity.csv','w') as output_file:
        with open('../data/pairs_for_matching.csv') as input_file:
            err = 0.0
            for line in input_file:
                user1, user2 = line.strip().split(",")
                for i in range(0,7):
                    for j in range(0, 24):
                        user1_cnt = 0
                        user2_cnt = 0
                        user1_total_hours = 0
                        user2_total_hours = 0
                        x = user1+str(i)+str(j)
                        y = user2+str(i)+str(j)
                        if x in daily_activity_count:
                            user1_cnt = int(daily_activity_count[x])
                            user1_total_hours = daily_hours_count[x]
                        if y in daily_activity_count:
                            user2_cnt = int(daily_activity_count[y])
                            user2_total_hours = daily_hours_count[y]

                        if not user1_total_hours == 0 and not user2_total_hours == 0:
                            err += ((user1_cnt/user1_total_hours) - (user2_cnt/user2_total_hours))**2
                        elif user1_total_hours == 0 and not user2_total_hours == 0:
                            err += (user2_cnt/user2_total_hours)**2
                        elif user2_total_hours == 0 and not user1_total_hours == 0:
                            err += (user1_cnt/user1_total_hours)**2

                err = np.sqrt(err)
                output_file.write("%s %s,%d\n" % (user1, user2, err))
    with open('../data/pairs_for_modeling_daily_activity2.csv','w') as output_file:
        with open('../data/pairs_for_modeling2.csv') as input_file:
            err = 0.0
            for line in input_file:
                user1, user2, match = line.strip().split(",")
                for i in range(0,7):
                    for j in range(0, 24):
                        user1_cnt = 0
                        user2_cnt = 0
                        user1_total_hours = 0
                        user2_total_hours = 0
                        x = user1+str(i)+str(j)
                        y = user2+str(i)+str(j)
                        if x in daily_activity_count:
                            user1_cnt = int(daily_activity_count[x])
                            user1_total_hours = daily_hours_count[x]
                        if y in daily_activity_count:
                            user2_cnt = int(daily_activity_count[y])
                            user2_total_hours = daily_hours_count[y]

                        if not user1_total_hours == 0 and not user2_total_hours == 0:
                            err += ((user1_cnt/user1_total_hours) - (user2_cnt/user2_total_hours))**2
                        elif user1_total_hours == 0 and not user2_total_hours == 0:
                            err += (user2_cnt/user2_total_hours)**2
                        elif user2_total_hours == 0 and not user1_total_hours == 0:
                            err += (user1_cnt/user1_total_hours)**2

                err = np.sqrt(err)
                output_file.write("%s %s,%d\n" % (user1, user2, err))


def generate_overlap_features():
    days_active = pickle.load(open("../data-train-dca/days_active.p", "rb"))
    hours_active = pickle.load(open("../data-train-dca/hours_active.p", "rb"))
    days_count = pickle.load(open("../data-train-dca/days_count.p", "rb"))
    hours_count = pickle.load(open("../data-train-dca/hours_count.p", "rb"))
    hours_active_indep = pickle.load(open("../data-train-dca/hours_active_indep.p", "rb"))

    with open('../data/pairs_for_matching_days_overlap.csv','w') as output_file, \
         open('../data/pairs_for_matching_hours_overlap.csv','w') as output_file2, \
         open('../data/pairs_for_matching_days_overlap_count.csv','w') as output_file3, \
         open('../data/pairs_for_matching_hours_overlap_count.csv','w') as output_file4, \
         open('../data/pairs_for_matching_hours_indep_overlap_count.csv','w') as output_file5:
        with open('../data/pairs_for_matching.csv') as input_file:
            for line in input_file:
                days_err = 0.0
                hours_err = 0.0
                
                user1, user2 = line.strip().split(",")
                set1 = days_active[user1]
                set2 = days_active[user2]
                day_overlap_count = len(set1.intersection(set2))
                output_file.write("%s %s,%d\n" % (user1, user2, day_overlap_count))

                set3 = hours_active[user1]
                set4 = hours_active[user2]
                hour_overlap_count = len(set3.intersection(set4))
                output_file2.write("%s %s,%d\n" % (user1, user2, hour_overlap_count))

                set5 = hours_active_indep[user1]
                set6 = hours_active_indep[user2]
                hour_indep_overlap_count = len(set5.intersection(set6))
                output_file5.write("%s %s,%d\n" % (user1, user2, hour_indep_overlap_count))

                dict1 = days_count[user1]
                dict2 = days_count[user2]
                keys_dict1 = set(dict1.keys())
                keys_dict2 = set(dict2.keys())
                shared_keys = keys_dict1.intersection(keys_dict2)
                for key in shared_keys:
                    a = dict1[key]
                    b = dict2[key]
                    days_err += (a - b)**2
                for key in (keys_dict1 - shared_keys):
                    a = dict1[key]
                    days_err += a**2
                for key in (keys_dict2 - shared_keys):
                    b = dict2[key]
                    days_err += b**2
                days_err = np.sqrt(days_err)
                output_file3.write("%s %s,%d\n" % (user1, user2, days_err))

                dict3 = hours_count[user1]
                dict4 = hours_count[user2]
                keys_dict3 = set(dict3.keys())
                keys_dict4 = set(dict4.keys())
                shared_keys2 = keys_dict3.intersection(keys_dict4)
                for key in shared_keys2:
                    a = dict3[key]
                    b = dict4[key]
                    hours_err += (a - b)**2
                for key in (keys_dict3 - shared_keys2):
                    a = dict3[key]
                    hours_err += a**2
                for key in (keys_dict4 - shared_keys2):
                    b = dict4[key]
                    hours_err += b**2
                hours_err = np.sqrt(hours_err)
                output_file4.write("%s %s,%d\n" % (user1, user2, hours_err))

    with open('../data/pairs_for_modeling_days_overlap2.csv','w') as output_file, \
         open('../data/pairs_for_modeling_hours_overlap2.csv','w') as output_file2, \
         open('../data/pairs_for_modeling_days_overlap_count2.csv','w') as output_file3, \
         open('../data/pairs_for_modeling_hours_overlap_count2.csv','w') as output_file4, \
         open('../data/pairs_for_modeling_hours_indep_overlap_count2.csv','w') as output_file5:
        with open('../data/pairs_for_modeling2.csv') as input_file:
            for line in input_file:
                days_err = 0.0
                hours_err = 0.0
                
                user1, user2, match = line.strip().split(",")
                set1 = days_active[user1]
                set2 = days_active[user2]
                day_overlap_count = len(set1.intersection(set2))
                output_file.write("%s %s,%d\n" % (user1, user2, day_overlap_count))

                set3 = hours_active[user1]
                set4 = hours_active[user2]
                hour_overlap_count = len(set3.intersection(set4))
                output_file2.write("%s %s,%d\n" % (user1, user2, hour_overlap_count))

                set5 = hours_active_indep[user1]
                set6 = hours_active_indep[user2]
                hour_indep_overlap_count = len(set5.intersection(set6))
                output_file5.write("%s %s,%d\n" % (user1, user2, hour_indep_overlap_count))

                dict1 = days_count[user1]
                dict2 = days_count[user2]
                keys_dict1 = set(dict1.keys())
                keys_dict2 = set(dict2.keys())
                shared_keys = keys_dict1.intersection(keys_dict2)
                for key in shared_keys:
                    a = dict1[key]
                    b = dict2[key]
                    days_err += (a - b)**2
                for key in (keys_dict1 - shared_keys):
                    a = dict1[key]
                    days_err += a**2
                for key in (keys_dict2 - shared_keys):
                    b = dict2[key]
                    days_err += b**2
                days_err = np.sqrt(days_err)
                output_file3.write("%s %s,%d\n" % (user1, user2, days_err))

                dict3 = hours_count[user1]
                dict4 = hours_count[user2]
                keys_dict3 = set(dict3.keys())
                keys_dict4 = set(dict4.keys())
                shared_keys2 = keys_dict3.intersection(keys_dict4)
                for key in shared_keys2:
                    a = dict3[key]
                    b = dict4[key]
                    hours_err += (a - b)**2
                for key in (keys_dict3 - shared_keys2):
                    a = dict3[key]
                    hours_err += a**2
                for key in (keys_dict4 - shared_keys2):
                    b = dict4[key]
                    hours_err += b**2
                hours_err = np.sqrt(hours_err)
                output_file4.write("%s %s,%d\n" % (user1, user2, hours_err))

def generate_interval_timerange_features():
    timerange = pickle.load(open("../data-train-dca/timerange.p", "rb"))
    access_intervals = pickle.load(open("../data-train-dca/access_intervals.p", "rb"))
    with open('../data/pairs_for_matching_timerange.csv','w') as output_file, \
         open('../data/pairs_for_matching_access_intervals.csv','w') as output_file2:
        with open('../data/pairs_for_matching.csv') as input_file:
            for line in input_file:
                user1, user2 = line.strip().split(",")
                tr1 = timerange[user1]
                tr2 = timerange[user2]
                diff = abs(tr2 - tr1)
                output_file.write("%s %s,%d\n" % (user1, user2, diff))

                ai1 = access_intervals[user1]
                ai2 = access_intervals[user2]
                diff2 = abs(ai2 - ai1)
                output_file2.write("%s %s,%d\n" % (user1, user2, diff2))
    with open('../data/pairs_for_modeling_timerange2.csv','w') as output_file, \
         open('../data/pairs_for_modeling_access_intervals2.csv','w') as output_file2:
        with open('../data/pairs_for_modeling2.csv') as input_file:
            for line in input_file:
                user1, user2, match = line.strip().split(",")
                tr1 = timerange[user1]
                tr2 = timerange[user2]
                diff = abs(tr2 - tr1)
                output_file.write("%s %s,%d\n" % (user1, user2, diff))

                ai1 = access_intervals[user1]
                ai2 = access_intervals[user2]
                diff2 = abs(ai2 - ai1)
                output_file2.write("%s %s,%d\n" % (user1, user2, diff2))

if __name__ == "__main__":
    # setup
    generate_activity_count()
    generate_active_count()
    generate_interval_timerange()

    # feature generation
    generate_activity_pair_features()
    generate_overlap_features()
    generate_interval_timerange_features()



     
     




