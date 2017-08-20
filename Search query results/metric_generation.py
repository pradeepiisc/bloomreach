
import urllib2,urllib
import re
import argparse
from mobile.backend.scripts.search.data_mining.data_mining_utils import bc2_utils as bc2u
from metric_preparation import *
#import random
parse_qd_line = lambda line,delimeters : re.split(delimeters,line.strip())

def main(qd_file,retrieved_products_file,solr_products_file,proportion_metric_file,qd_ratio_metric_file):
  qry_prdct_score_dict = get_qpd_dictionary(qd_file)
  print 'Created the Query:{product:Score} dictionary'

  '''
  Unit Test 
  random_keys = random.sample(qry_prdct_score_dict,25)
  qry_prdct_score_dict = dict((k,qry_prdct_score_dict[k]) for k in random_keys)
  '''

  #Reading retrieved products from the retrieved products file
  qp_dict = get_retrieved_products_dictionary(retrieved_products_file)
  print 'Retrieved all the products from url for all distinct queries'

  solr_prdcts_set = get_solr_products(solr_products_file)

  proportion_metric_dict,qd_score_metric_dict = get_metrics_helper(qry_prdct_score_dict,qp_dict,solr_prdcts_set)
  write_proportion_metric_to_file(proportion_metric_dict,proportion_metric_file)
  write_qd_ratio__metric_to_file(qd_score_metric_dict,qd_ratio_metric_file)
  print 'Metric generation done in files ',qd_ratio_metric_file,proportion_metric_file

'''
reads the retrieved_products_file and returns dictionary of the form {query:products}
'''
def get_retrieved_products_dictionary(retrieved_products_file):
  delimeters = '[\t]'
  qp_file_handler = open(retrieved_products_file)
  qp_dict = {}
  for line in qp_file_handler:
    lineResult = parse_qd_line(line,delimeters)
    query = lineResult[0]
    product_ids = lineResult[2:]
    qp_dict[query] = qp_dict.get(query,set())
    qp_dict[query].update(product_ids)

  qp_file_handler.close()
  return qp_dict

'''
get_solr_products reads the sol
'''
def get_solr_products(solr_prdcts_file):
  solr_products = set()
  file_handler = open(solr_prdcts_file)
  for each_product in file_handler:
    solr_products.update(each_product.strip())

  return solr_products

def write_proportion_metric_to_file(proportion_metric_dict,proportion_metric_file):
  metric_file_handler = open(proportion_metric_file,'w')
  metric_file_handler.write('Query\tRatio of retrieved products to total products\n')
  for query in proportion_metric_dict.keys():
    write_metric_to_file(metric_file_handler,query,str(proportion_metric_dict[query]),'\t')

  metric_file_handler.close()

def write_qd_ratio__metric_to_file(qd_score_metric_dict,qd_ratio_metric_file):
  metric_file_handler = open(qd_ratio_metric_file,'w')
  metric_file_handler.write('Query\tRatio of qdScore of retrieved Products to qdScore of all Products\n')
  for query in qd_score_metric_dict.keys():
    write_metric_to_file(metric_file_handler,query,str(qd_score_metric_dict[query]),'\t')

  metric_file_handler.close()

'''
get_metrics generates the metric 
'''
def get_metrics_helper(qry_prdct_score_dict,retrieved_qry_prdct_dict,solr_prdcts_set):
  qd_score_metric_dict = {}
  proportion_metric_dict = {}
  queries = retrieved_qry_prdct_dict.keys()
  for query in queries:
    prdct_score_dict = qry_prdct_score_dict.get(query,{})
    retrieved_product_set = retrieved_qry_prdct_dict[query]
    universal_product_set = set(prdct_score_dict.keys())
    universal_product_set = universal_product_set.intersection(solr_prdcts_set)
    qd_score_metric_dict[query] = get_qd_score_ratio_metric(prdct_score_dict,retrieved_product_set,universal_product_set)
    proportion_metric_dict[query] = get_proportion_ratio_metric(retrieved_product_set,universal_product_set)

  return proportion_metric_dict,qd_score_metric_dict

def get_qd_score_ratio_metric(prdct_score_dict,retrieved_product_set,universal_product_set):
  numerator_qd_score = 0
  denominator_qd_score  = 0
  qd_ratio_metric = 0
  denominator_qd_score = set( prdct_score_dict[pd] for pd in universal_product_set )
  denominator_qd_score = sum(map(float,denominator_qd_score))
  for product in retrieved_product_set:
    if product in prdct_score_dict.keys():
      numerator_qd_score += float(prdct_score_dict[product])
    else:
      print product + 'Not Found\n'

  if denominator_qd_score != 0:
    qd_ratio_metric = numerator_qd_score / denominator_qd_score

  return qd_ratio_metric

def get_proportion_ratio_metric(retrieved_product_set,universal_product_set):
  num_retrieved_products = 0
  count_ratio_metric = 0
  total_products = len(universal_product_set)
  num_retrieved_products = len(retrieved_product_set)
  
  #write_metric_to_file(metric_file_handler,str(numerator_qd_score),str(denominator_qd_score),str(qd_ratio_metric),'\t')
  if total_products != 0:
    count_ratio_metric = float(num_retrieved_products) / total_products
  #write_metric_to_file(metric_file_handler,str(num_retrieved_products),str(total_products),str(count_ratio_metric),'\n')
  return count_ratio_metric
  
def write_metric_to_file(file_handler,query,metric,delimeter):
  file_handler.write(query+ delimeter+ metric + '\n')

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = "retrieve product ids for queries and generate metrics")
  parser.add_argument('--qd_file',  required=True, help="file containing query product qd score")
  parser.add_argument('--retrieved_product_file',  required=True, help="retrieved product file")
  parser.add_argument('--solr_products_file',  required=True, help="solr_products_file")
  parser.add_argument('--proportion_metric_file',  required=True, help="proportion metric file")
  parser.add_argument('--qd_ratio_metric_file',  required=True, help="qd ratio metric file")
  args = parser.parse_args()
  main(args.qd_file,args.retrieved_product_file,args.solr_products_file,args.proportion_metric_file,args.qd_ratio_metric_file)
