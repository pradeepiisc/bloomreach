import urllib2,urllib
import re
import argparse
from mobile.backend.scripts.search.data_mining.data_mining_utils import bc2_utils as bc2u
#import random
parse_qd_line = lambda line,delimeters : re.split(delimeters,line.strip())

def main(qd_file, merchant_domain,merchant_collection,retrieved_product_file,solr_products_file):
  qry_prdct_score_dict = get_qpd_dictionary(qd_file)
  print 'Created the Query:{product:Score} dictionary'
  solr_url,req_id = get_working_url(merchant_domain,merchant_collection)
  print 'Cluster spawned at:',solr_url
  write_retrieved_products(qry_prdct_score_dict,solr_url,retrieved_product_file)
  print 'Retrieved all the products from url for all distinct queries'
  solr_prdcts_set = generate_solr_products_set(solr_url,merchant_collection)
  write_solr_products(solr_prdcts_set,solr_products_file)

  terminate_cluster(req_id)
  print 'Terminated the cluster'


'''
The below function reads the qd file and returns the dictionary of kind {query:{product:score}}
'''
def get_qpd_dictionary(qd_file):
  qd_file_handler = open(qd_file)
  qry_prdct_score_dict = {} 
  delimeters = '[=:]'
  for line in qd_file_handler:
    lineResult = parse_qd_line(line,delimeters)
    product = lineResult[0]
    query = lineResult[1]
    score = lineResult[-1]
    qry_prdct_score_dict[query] = qry_prdct_score_dict.get(query,{})
    qry_prdct_score_dict[query].update({product:score})

  qd_file_handler.close()
  return qry_prdct_score_dict

'''
Below function takes the dictionary of {query:{product:score}} and 
writes retrieved products for each query(all the keys of dictionary) into the file
'''
def write_retrieved_products(qry_prdct_score_dict,solr_url,output_file):
  #Writing retrieved products from solr into the output_file
  queries = get_queries(qry_prdct_score_dict)
  qp_file_handler = open(output_file,'wb')
  #qp_dict = {}
  count = 0
  solr_url_specified_rows = solr_url + 'rows=1000&facet=false&'
  for query in queries:
    count += 1
    prdct_score_dict = qry_prdct_score_dict.get(query,{})
    fq_products = prdct_score_dict.keys()
    url_result = get_result(solr_url_specified_rows,query,fq_products)
    if url_result != None:
      results = get_writable_result(url_result)
      qp_file_handler.write(query + '\t' + results + '\n')
    else:
      print 'error' , query
  qp_file_handler.close()
  
def write_solr_products(solr_prdcts_set,solr_products_file):
	solr_products_file_handler = open(solr_products_file,'wb')
	for each_product in solr_prdcts_set:
		solr_products_file_handler.write(each_product+'\n')

	solr_products_file_handler.close()

def generate_solr_products_set(solr_url):
  url_query = solr_url + '&rows=1000000000&q=*'
  url_result = get_url_result(url_query)
  url_result_set = set(url_result.strip().split('\n'))
  url_result_set.remove('product_id')
  return url_result_set

def terminate_cluster(req_id):
  bc2u.submit_bc2_de_provision_request(req_id)

def get_working_url(merchant_domain,merchant_collection):
  req_id = bc2u.submit_bc2_provision_request(merchant_domain,[merchant_collection])
  solr_url = bc2u.get_bc2_solr_url(req_id)
  solr_url = solr_url.split(',')[0]
  print solr_url
  solr_url = 'http://' + solr_url + '/solr/' + merchant_collection + '/browse_anchor?fl=product_id&wt=csv&'
  return solr_url,req_id

def get_queries(qp_qd_dict):
  return qp_qd_dict.keys()

def get_result(solr_url,query,fq_products):
  modified_query = urllib.urlencode({"q":query})
  fq = get_fq_string(fq_products)
  complete_url = solr_url + modified_query + '&' + fq
  return get_url_result(complete_url)

def get_url_result(url_query):
  try:
    url_request = urllib2.Request(url_query)
    url_result = urllib2.urlopen(url_request)
    url_result = url_result.read()
  except:
    return None

  return url_result

def get_fq_string(fq_products):
  fq_product_string = ""
  for product in fq_products:
    fq_product_string = fq_product_string + 'product_id:' + product + ' OR '

  fq_product_string = fq_product_string.rstrip(' OR ')
  return urllib.urlencode({"fq":fq_product_string})

def get_writable_result(url_result):
  return "\t".join([line.strip() for line in url_result.split('\n')])
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = "retrieve product ids for queries and write it into file")
  parser.add_argument('--qd_file',  required=True, help="file containing query product qd score")
  parser.add_argument('--merchant_domain',  required=True, help="merchant domain")
  parser.add_argument('--merchant_collection',  required=True, help="merchant collection")
  parser.add_argument('--retrieved_product_file',  required=True, help="retrieved product file")
  parser.add_argument('--solr_products_file',  required=True, help="solr products file")
  args = parser.parse_args()
  main(args.qd_file,args.merchant_domain,args.merchant_collection,args.retrieved_product_file,args.solr_products_file)

