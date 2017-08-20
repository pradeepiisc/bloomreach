

from __future__ import print_function

import sys
import argparse
from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec
import ConfigParser
from collections import namedtuple
import bigrams as bigram_util

FILE_FORMAT="org.apache.hadoop.io.compress.GzipCodec"

def map_bigram_to_line_num(line_tuple):
  ''' Maps bigrams to line number

  Given a tuple(line_num,line)
  it returns a list of 
  bigrams in the line
  '''
  line_number,line = line_tuple[0],line_tuple[1]
  bigrams = bigram_util.get_bigrams(line)
  bigrams_list = []
  for bigram in bigrams:
    bigrams_list.append([bigram,line_number])
  return bigrams_list


'''
Given a line and a list of bigrams
if any bigram is present in the line
space of the bigram is replaced with
an underscore
'''
def insert_bigrams(line,bigrams):
  if bigrams == None:
    return line
  for vb in bigrams:
    line = line.replace(vb,vb.replace(' ','_'))
  return line

'''
line_num_to_line: 
rdd with line_number to line
Returns an rdd with a map of 
bigram to its line_number
(<bigram>,('valid',<line_number>) where the <bigram> is present)
'''
def get_bigram_to_line_num(bigrams,line_num_to_line):
  bigram_map = bigrams.map(lambda bigram : (bigram,'valid'))
  bigram_to_line_num = line_num_to_line.flatMap(map_bigram_to_line_num)
  bigram_to_line_num = bigram_map.join(bigram_to_line_num)
  return bigram_to_line_num

'''
convert an rdd with lines to 
rdd with line_number to line
Output is an rdd with tuples
of the form (line_number,line)
'''
def get_line_num_to_line(inp):
  line_num_to_line = inp.zipWithIndex()
  line_num_to_line = line_num_to_line.map(lambda (line,indx) : (indx,line))
  return line_num_to_line

'''
given a rdd of map from 
line_num to line and
rdd of map from 
bigrams to line num
it returns the rdd
of line with their corresponding bigrams
'''
def get_line_with_bigrams(line_num_to_line,bigram_to_line_num):
  #bigram_to_line_num has tuples of the form (bigram,('valid',line_num))
  line_num_to_single_bigram = bigram_to_line_num.map(lambda z: (z[1][1],z[0]))
  line_num_to_bigrams = line_num_to_single_bigram.groupByKey()
  line_num_to_bigrams = line_num_to_bigrams.map(lambda z : (z[0],tuple(z[1])))
  line_with_bigrams = line_num_to_line.leftOuterJoin(line_num_to_bigrams)
  return line_with_bigrams

'''
Input : inp - rdd of input text 
Output : rdd of input text
with frequent n-grams discovered 
'''
def insert_n_grams(inp,min_count,bigram_threshold):
  valid_bigrams = bigram_util.get_valid_bigrams(inp,min_count,bigram_threshold)
  line_number_to_line = get_line_num_to_line(inp)
  valid_bigrams_to_respective_line_num = get_bigram_to_line_num(valid_bigrams,line_number_to_line)
  line_with_its_valid_bigrams = get_line_with_bigrams(line_number_to_line,valid_bigrams_to_respective_line_num)
  input_with_valid_ngrams = line_with_its_valid_bigrams.map(lambda z: insert_bigrams(z[1][0],z[1][1]))
  return input_with_valid_ngrams

def set_word_to_vec_params(word_to_vec,param_obj):
  word_to_vec.setMinCount(param_obj.min_count)
  #word_to_vec.setWindowSize(param_obj.skip_window) #no such method supported by word2vec
  word_to_vec.setNumIterations(param_obj.iterations)
  word_to_vec.setNumPartitions(param_obj.partitions)
  word_to_vec.setVectorSize(param_obj.vector_size)
  return

'''
Input : train_data-Training data
Output : model - model trained on train_data
using Word2Vec()
'''
def train_model(train_data,param_obj):
  word2vec = Word2Vec()
  set_word_to_vec_params(word2vec,param_obj)
  model = word2vec.fit(train_data)
  return model


'''
Input : word_to_vec_map - map from word 
to vector
Output : list with word and 
its vector representation
'''
def get_dict_in_list_format(word_to_vec_map):
  wordvectors = []
  for key in word_to_vec_map:
    tup = [key] + list(word_to_vec_map[key])
    wordvectors.append(tup)
  return wordvectors

'''
Input : word_to_vec_map - map from word 
to vector
output_file_location - location where embeddings
will be stored
file_format - file format in which embeddings will
be stored
Output : NOTHING
'''
def write_word_vectors_to_file(word_to_vec_map,output_file_location,file_format):
  wordvectors = get_dict_in_list_format(word_to_vec_map)
  rdd = sc.parallelize(wordvectors)
  rdd = rdd.map(lambda x:",".join(map(unicode, x)))
  rdd.saveAsTextFile(path=output_file_location, compressionCodecClass=file_format)
  return
'''
Input : inp - rdd of input text  
param_obj - structure with Word2Vec param values
Output : Map frm Word to their vector representations
'''
def generate_embeddings(inp,param_obj):
  model = train_model(inp,param_obj)
  word_to_vec_map = model.getVectors()
  return word_to_vec_map

'''
Input : inp - rdd of input text 
file_out_path - path where output will be stored
conf_file - file with Word2Vec parameter values
Output : Word and their vector representations are
written in the file in the given format
'''
def write_embeddings_to_file(inp,file_out_path,param_obj):
  bigram_threshold = param_obj.bigram_threshold
  min_count = param_obj.min_count
  inp = insert_n_grams(inp,min_count,bigram_threshold).map(lambda row : row.split(" "))
  word_to_vec_map = generate_embeddings(inp,param_obj)
  write_word_vectors_to_file(word_to_vec_map,file_out_path,FILE_FORMAT)



if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Generate Embeddings for n-grams using Word2Vec')
  parser.add_argument('--train_file', help="File with data for training", required=True)
  parser.add_argument('--output_directory', help="Directory where word and its vector will be stored", required=True)
  
  args = parser.parse_args()
  sc = SparkContext(appName='Word2Vec')
  inp = sc.textFile(args.train_file)
  config_struct = namedtuple("params", "min_count vector_size partitions iterations bigram_threshold window_size")
  cs = config_struct(10,20,1,1,150.0,5)
  (inp,args.output_directory,cs)
  sc.stop()
