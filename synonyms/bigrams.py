"""
Copyright 2016 BloomReach.com. All rights reserved
Author(s): Pradeep Bansal (pradeep.bansal@bloomreach.com)
Date: April 11th, 2016
""" 

from operator import add

'''
Input: rdd with file input in raw form
Output : returns an rdd with bigrams
'''
def get_valid_bigrams(file_rdd,min_count,bigram_threshold):
  bigram_count = get_bigram_with_count(file_rdd)
  unigram_count = get_unigram_with_count(file_rdd)
  bigrams_with_score = get_bigrams_with_score(bigram_count,unigram_count,min_count)
  valid_bigrams_with_score = bigrams_with_score.filter(lambda x : x[1] > bigram_threshold)
  valid_bigrams = valid_bigrams_with_score.map(lambda x : x[0])
  return valid_bigrams
 

'''
Input  : rdd of n-grams
Output : rdd of (n-gram,its_count)
'''
def count_grams(gram_rdd):
  counts = gram_rdd.map(lambda x: (x,1))\
      .reduceByKey(add)
  return counts

'''
Input  : "line" for which bigrams are required
Output : all possible bigrams in the line
'''
def get_bigrams(line):
  words = line.split()
  return (w1 + ' ' + w2 for w1,w2 in zip(words,words[1:]))

'''
Input : x is a tuple containing 
phrase,its count,counts of unigrams present
in the phrase
total_words - total unigrams present in 
the input
Output : tuple - (phrase,phrase_score)
'''
def calculate_bigram_score(bigram_unigram_combined_info,min_count,total_unigrams):
  #bigram_unigram_combined_info="(( ( 'a b',pab ),pa ), pb )"
  b_count  = bigram_unigram_combined_info[1]
  a_count  = bigram_unigram_combined_info[0][1]
  ab_count = bigram_unigram_combined_info[0][0][1]
  bigram = bigram_unigram_combined_info[0][0][0]
  return (bigram,(ab_count - min_count) / float(a_count) / float(b_count) * float(total_unigrams))

def get_total_unigrams(rdd_unigram_count):
  return rdd_unigram_count.map(lambda j: j[1]).sum()

'''
Input : 
x - tuple with 2 or 3 entries
(<string>,count)
left - decides the course of output
lhs,rhs = <string>.split()

left = True then "lhs" will be
first entry in the output tuple
left = False then "rhs" will be
first entry in the output tuple

Output : tuple with first entry 
from first part of <string>.split()
and second entry as x itself
'''
def split_tuple_key(x,left = True):
  if left:
    key = x[0]
  else:
    key_tuple = x[1][0]
    key = key_tuple[0]
  key_lhs,key_rhs = key.split(' ')
  if left:
    return (key_lhs,(key,x[1]))
  else:
    return (key_rhs,x[1])

def get_bigrams_with_score(rdd_bigram_with_count,rdd_unigram_with_count,min_count):
  bigram_key_split = rdd_bigram_with_count.map(split_tuple_key)
  bigram_unigram_combined = bigram_key_split.join(rdd_unigram_with_count)
  bigram_unigram_combined = bigram_unigram_combined.map(lambda j:split_tuple_key(j,False))
  bigram_unigram_combined = bigram_unigram_combined.join(rdd_unigram_with_count)
  total_unigrams = get_total_unigrams(rdd_unigram_with_count)
  bigrams_with_count = bigram_unigram_combined.map(lambda x : calculate_bigram_score(x[1],min_count,total_unigrams) )
  return bigrams_with_count  

'''
Input  : rdd returned after reading a file
Output : rdd with unigram and its count
an entry in output rdd looks like 
(unigram,unigram_count)
'''
def get_unigram_with_count(file_rdd):
  unigrams = file_rdd.flatMap(lambda x:x.split(' '))
  return count_grams(unigrams)

'''
Input  : rdd returned after reading a file
Output : rdd with bigrams and their count
an entry in output rdd looks like
(bigram,bigram_count)
'''
def get_bigram_with_count(file_rdd):
  bigrams = file_rdd.flatMap(get_bigrams)
  return count_grams(bigrams)


