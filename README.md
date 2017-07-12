# README 

For CS249 cross device entity resolution class project. 

## EXPLANATION OF FILES

### OVERVIEW

Our code is broken up into three main parts: data parsing, feature generation and modeling/matching. The code for parsing and feature generation will take a really long time to run (several days in total probably). These code files are just used to generate data for modeling and matching. They do not need to be run to derive the final solution, since we provide the output files for these programs.

The main component of our code is matching.py, which selects optimal features, builds a model from feature data, performs matching and generates submission.txt, a list of matching pairs which serves as the entity resolution solution. It only takes a few minutes to run. 

Running matching.py:
1) Download data folder (https://www.dropbox.com/s/ui4qzupvf0hhenf/data.zip?dl=0), unzip it and place it in the same directory as matching.py. 
2) In a terminal window enter: python matching.py

Note: The following libraries are required for matching.py: sklearn, numpy and pandas. Other libraries may be required (see top of matching.py). 

### PARSING

parse.py -- This file takes the raw data files provided by CIKM and converts it to pickle files, which are used by other programs in later stages. In order to run this program, in the parent folder of these files, place a folder called "data-train-dca" with "urls.csv", "train.csv", "titles.csv" and "facts.json". These can be downloaded here: https://drive.google.com/drive/u/0/folders/0B7XZSACQf0KdNXVIUXEyVGlBZnc. 
time_parse.py -- This file generates pickle files for time specific features. Make sure to have the same directory structure as the one mentioned for parse.py

### BLOCKING

blocking.py -- This file takes all the users in facts.json and generates a list of the most likely 1,044,396 matching pairs based on tf-idf matrix generated from the url domains of the users. It takes at least one hour to run. parse.py must be run before running this program.

### FEATURE GENERATION

gen_features.title.py -- This file generates feature data related to websites titles. It takes at 3 or 4 hours to run. 

time_feature_generation.py --  This file generates feature data related to timestamps. 

### MODELING AND MATCHING

gen_model_pairs.py -- This takes the user pairs in train.csv and randomly matched users in facts.json to create a list of 200,000 negative and positive matches for the model creation step. This takes about half an hour to run.

matching.py -- This is the primary file for our program. It uses our feature data to create a model and then applies that model to the matching pairs to generation a submission file. This file takes a couple of minutes to run. 

urlScripts folder -- generates URL features 

## REQUIRED LIBRARIES

You'll need sklearn (http://scikit-learn.org/stable/), pandas and maybe some other stuff. See import list atop files.

## OTHER
Overleaf: https://www.overleaf.com/8434780nrmbhdtwhpbm#/29934283/
Data Files: https://competitions.codalab.org/competitions/11171#learn_the_details-data2
