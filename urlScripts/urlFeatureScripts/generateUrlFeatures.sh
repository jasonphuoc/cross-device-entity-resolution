#!/usr/bin/env bash
# for i in {0..199}
# do
#   echo "processing modeling pairs"
#   if [ "$i" -lt 10 ]
#   then
#     node 1modIndexPairs.js "../data/pairs_for_modeling2Files/pairs_for_modeling200$i.csv"
#   elif [ "$i" -lt 100 ]
#   then
#     node 1modIndexPairs.js "../data/pairs_for_modeling2Files/pairs_for_modeling20$i.csv"
#   else
#     node 1modIndexPairs.js "../data/pairs_for_modeling2Files/pairs_for_modeling2$i.csv"
#   fi
# done
# echo "done processing modeling pairs!"

# for i in {0..1044}
# do
#   echo "processing matching pairs"
#   if [ "$i" -lt 10 ]
#   then
#     node 1matIndexPairs.js "../data/pairs_for_matchingFiles/pairs_for_matching000$i.csv"
#   elif [ "$i" -lt 100 ]
#   then
#     node 1matIndexPairs.js "../data/pairs_for_matchingFiles/pairs_for_matching00$i.csv"
#   elif [ "$i" -lt 1000 ]
#   then
#     node 1matIndexPairs.js "../data/pairs_for_matchingFiles/pairs_for_matching0$i.csv"
#   else
#     node 1matIndexPairs.js "../data/pairs_for_matchingFiles/pairs_for_matching$i.csv"
#   fi
# done
# echo "done processing matching pairs!"

# for i in {0..339}
# do
#   echo "processing facts"
#   if [ "$i" -lt 10 ]
#   then
#     node 2indexFacts.js "../data/factFiles/facts00$i.json"
#   elif [ "$i" -lt 100 ]
#   then
#     node 2indexFacts.js "../data/factFiles/facts0$i.json"
#   else
#     node 2indexFacts.js "../data/factFiles/facts$i.json"
#   fi
# done
# echo "done processing facts!"
#
# for i in {0..191}
# do
#   echo "processing urls"
#   if [ "$i" -lt 10 ]
#   then
#     node 3indexUrls.js "../data/splitUrlFiles/urls00$i.csv"
#   elif [ "$i" -lt 100 ]
#   then
#     node 3indexUrls.js "../data/splitUrlFiles/urls0$i.csv"
#   else
#     node 3indexUrls.js "../data/splitUrlFiles/urls$i.csv"
#   fi
# done
# echo "done processing urls!"

for i in {0..197}
do
  echo "creating url modeling features"
  if [ "$i" -lt 10 ]
  then
    node 4modGenerateUrlFeatures.js "../data/pairs_for_modelingFiles/pairs_for_modeling00$i.csv"
  elif [ "$i" -lt 100 ]
  then
    node 4modGenerateUrlFeatures.js "../data/pairs_for_modelingFiles/pairs_for_modeling0$i.csv"
  else
    node 4modGenerateUrlFeatures.js "../data/pairs_for_modelingFiles/pairs_for_modeling$i.csv"
  fi
done
echo "done creating url modeling features!"

for i in {0..1044}
do
  echo "creating url matching features"
  if [ "$i" -lt 10 ]
  then
    node 4matGenerateUrlFeatures.js "../data/pairs_for_matchingFiles/pairs_for_matching000$i.csv"
  elif [ "$i" -lt 100 ]
  then
    node 4matGenerateUrlFeatures.js "../data/pairs_for_matchingFiles/pairs_for_matching00$i.csv"
  elif [ "$i" -lt 1000 ]
  then
    node 4matGenerateUrlFeatures.js "../data/pairs_for_matchingFiles/pairs_for_matching0$i.csv"
  else
    node 4matGenerateUrlFeatures.js "../data/pairs_for_matchingFiles/pairs_for_matching$i.csv"
  fi
done
echo "done creating url matching features!"
