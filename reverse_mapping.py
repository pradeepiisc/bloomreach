'''
UNLOAD('SELECT browse_session_query, count(browse_session_products_viewed) as numberOfTimes from staples.browse_sessions where browse_session_products_viewed=''1104593'' group by browse_session_query') to 's3://brp-mobile/coverage/redshift/staples/browse_sessions_reverse_mappng/'
credentials 'aws_access_key_id=AKIAIBS6WJXTFANJPDYA;aws_secret_access_key=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' delimiter as '\t' allowoverwrite ;

UNLOAD('SELECT browse_session_query, browse_session_products_viewed from staples.browse_sessions where browse_session_products_viewed != ''(not set)'' ') to 's3://brp-mobile/coverage/redshift/staples/browse_sessions_query_products_viewed_mapping/'
credentials 'aws_access_key_id=AKIAIBS6WJXTFANJPDYA;aws_secret_access_key=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' delimiter as '\t' allowoverwrite ;

'''
import argparse
import operator
import re
parse_qd_line = lambda line,delimeters : re.split(delimeters,line.strip())
def main(query_viewed_products_file,query_less_results_file,qd_file,output_file):
	product_queries_dict,queries_count_dict,queries_products_dict = make_product_to_queries_dictionary(query_viewed_products_file)
	'''
	fh = open('fileForTesting','w')
	sorted_q_count = sorted(queries_count_dict.items(), key=operator.itemgetter(1),reverse=True)
	print sorted_q_count

	fh.close()
	return
	'''
	#print 'line15',product_queries_dict
	print 'read viewed products file'
	query_missing_products_dict = make_query_to_products_dictionary(query_less_results_file)
	#print 'line 19',query_missing_products_dict
	print 'read missing products file'
	qd_dict = get_qpd_dictionary(qd_file)
	#print 'line22',qd_dict
	print 'read qpd file'
	find_mapping_queries(query_missing_products_dict,product_queries_dict,queries_count_dict,qd_dict,queries_products_dict,output_file)
	print 'generated query mapping'

def write_to_file(query,query_mapping,output_file,file_handler):
	#print query_mapping
	#file_handler.write(query+'\t'+query_mapping[0][0] + '\t' + str(query_mapping[0][1]) + '\n')
	file_handler.write(query+'\t'+"\t".join(query_mapping) + '\n')
	return file_handler


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
		if float(score) > 2.0:
			qry_prdct_score_dict[query] = qry_prdct_score_dict.get(query,{})
			qry_prdct_score_dict[query].update({product:score})
	
	qd_file_handler.close()
	return qry_prdct_score_dict
def find_mapping_queries(query_missing_products_dict,product_queries_dict,query_dict,qd_dict,query_products_dict,output_file):
	file_handler = open(output_file,"w")
	query_products_dict_normalized = normalize_query_product_scores(query_products_dict)
	#print 'line 59',query_products_dict_normalized['dell 1130 toner cartridge']
	#print 'line 60',query_products_dict_normalized['copy paper']
	for less_results_query in query_missing_products_dict.keys():
		#if less_results_query != 'tn 720':
		#	continue
		missing_products = query_missing_products_dict[less_results_query]

		print 'line 53less result query-missing products',less_results_query,missing_products
		products_to_score_dict = get_qd_scores(qd_dict,less_results_query)
		#print 'line 55',products_to_score_dict
		if products_to_score_dict == None:
			continue
		#print 'line 56',products_to_score_dict
		#print 'line 57',missing_products
		#products_to_score_dict has super set of products in missing_products
		products_to_score_dict = normalize_qd_scores(products_to_score_dict)
		#print 'line 60',products_to_score_dict
		product_to_query_normalized_scores = normalised_query_to_product_importance(product_queries_dict,missing_products)
		#print 'line 65',query_products_dict
		
		#print 'line 62',product_to_query_normalized_scores
		#print 'line 67',query_products_dict_normalized
		query_mapping_ranked = get_ranked_queries(products_to_score_dict,product_to_query_normalized_scores,query_products_dict_normalized,missing_products)
		#query_mapping[less_results_query] = get_queries(product_queries_dict,missing_products[0],query_dict)
		#print 'line 65',less_results_query,query_mapping_ranked[less_results_query]
		file_handler = write_to_file(less_results_query,query_mapping_ranked,output_file,file_handler)
		#if less_results_query == 'tn 720':
		#	file_handler.close()
		#	return
		#print 'line 66 generated query mapping for query ',less_results_query
	file_handler.close()

def normalize_query_product_scores(query_products_dict):
	#print 'line 76',query_products_dict
	query_products_dict_normalized = {}
	for k in query_products_dict.keys():	#for each query
		#print 'line 79',k
		product_count_dict = query_products_dict[k]	
		#print 'line 80',product_count_dict
		#print 'line 81',missing_products
		query_products_dict_normalized[k] = query_products_dict_normalized.get(k,{})
		#total_sum = 0.0
		mp_intersection = product_count_dict.keys()
		#mp_intersection = set(missing_products).intersection(set(product_count_dict.keys()))
		#for p in mp_intersection:
			#total_sum += product_count_dict[p]
		total_sum = sum(product_count_dict.values()) 
		
		for p in mp_intersection: 
			query_products_dict_normalized[k][p] = query_products_dict_normalized[k].get(p,0.0)
			query_products_dict_normalized[k][p] = float(product_count_dict[p])/total_sum

	#print query_products_dict_normalized
	return query_products_dict_normalized

def normalised_query_to_product_importance(pq_dict,missing_products):
	temp_dict = {}
	for p in missing_products:
		temp_dict[p] = temp_dict.get(p,{})
		if p in pq_dict.keys():
			queries = pq_dict[p]
		else:
			continue
		total_sum = sum(queries.values())
		for k in queries:
			temp_dict[p][k] = temp_dict[p].get(k,0.0)
			temp_dict[p][k] = float(pq_dict[p][k]) / total_sum

	return temp_dict


def normalize_qd_scores(products_to_score_dict):
	total_sum = 0.0
	temp_dict = {}
	total_sum = [float(v) for v in products_to_score_dict.values()]
	total_sum = sum(total_sum)
	for p in products_to_score_dict.keys():
		temp_dict[p] = temp_dict.get(p,0.0)
		temp_dict[p] = float(products_to_score_dict[p]) / total_sum
	return temp_dict

'''
def normalize_qd_scores(products_to_score_dict,missing_products):
	total_sum = 0.0
	temp_dict = {}
	for p in missing_products:
		if p in products_to_score_dict.keys():
			total_sum += float(products_to_score_dict[p])
		else:
			print p + 'not found in mising products in line 127'
	for p in missing_products:
		if p in products_to_score_dict.keys():
			temp_dict[p] = temp_dict.get(p,0.0)
			temp_dict[p] = float(products_to_score_dict[p]) / total_sum
		else:
			temp_dict[p] = temp_dict.get(p,0.001)
	return temp_dict
'''

def get_qd_scores(qd_dict,query):
	if query in qd_dict.keys():
		return qd_dict[query]
	else:
		return None

def filter_query(query):
	#if product- occurs anywhere in a qurey
	#if query.find('product-') == -1 and query.find('staples.com') == -1 and not query.isdigit():
	if query.find('product-') == -1 and not query.isdigit():
		return True
	else:
		return False
def get_ranked_queries(qd_scores,product_to_query_normalized_scores,query_products_dict_normalized,missing_products):
	queries_dict = {}
	#fh = open('fileForTesting','w')
	#fh.write('Missing Product \t qd Score\n')
	for missed_product in missing_products:
		#print 'line 105',missed_product
		qd = qd_scores[missed_product]
		#fh.write(missed_product + ' \t ' + str(qd)+'\n')
		#print 'line 107-qd',qd
		queries = product_to_query_normalized_scores[missed_product]
		#print 'line 109',queries
		#fh.write('Query \t productToQuery \t QueryToProduct\n')
		for k in queries.keys():
			queries_dict[k] = queries_dict.get(k,0.0)
			#print k,missed_product
			queries_dict[k] += qd * queries[k] * query_products_dict_normalized[k][missed_product]
			#fh.write(k + ' \t ' + str(queries[k]) + ' \t ' + str(query_products_dict_normalized[k][missed_product]) + '\n')
			#print k,missed_product,query_products_dict_normalized[k][missed_product], queries_dict[k]
		#fh.write('\n-----Finshed one missed product------\n')

	#fh.close()
	#print 'line128',queries_dict
	sorted_queries = sorted(queries_dict.items(), key=operator.itemgetter(1),reverse=True)
	sorted_queries = [s[0] for s in sorted_queries]

	return sorted_queries

def get_queries(product_queries_dict,product,query_dict):#remove query_dict param
	query_count_dict = product_queries_dict[product]
	#meanwhile returing just the queries without the count
	#for k in query_count_dict.keys():
	#	print query_count_dict[k],query_dict[k]
	return query_count_dict.keys()

def get_key_value(line):
	tokens_in_line = line.strip().split('\t')
	query = tokens_in_line[0]
	products = tokens_in_line[1:]
	products = products[0].split(',')
	return query,products

def make_query_to_products_dictionary(query_less_results_file):
	qp_dict = {}
	file_handler = open(query_less_results_file)
	for line in file_handler:
		query,products = get_key_value(line)
		#this particular file has this structure
		#qp_dict[query] = qp_dict.get(query,[])
		if filter_query(query):
			products = [p.strip() for p in products]
			qp_dict[query] = products
		else:
			continue
	file_handler.close()
	return qp_dict

def make_product_to_queries_dictionary(query_products_file):
	pq_dict = {}
	query_dict = {}
	query_products_dict = {}
	file_handler = open(query_products_file)
	for line in file_handler:
		query,products = get_key_value(line)
		if filter_query(query):
			query_dict[query] = query_dict.get(query,0)
			query_products_dict[query] = query_products_dict.get(query,{})
			query_dict[query] += 1
			for p in products:
				pq_dict[p] = pq_dict.get(p,{})
				pq_dict[p][query] = pq_dict[p].get(query,0)
				pq_dict[p][query] += 1
				query_products_dict[query][p] = query_products_dict[query].get(p,0)
				query_products_dict[query][p] += 1
		else:
			continue

	#print query_products_dict
	file_handler.close()
	return pq_dict,query_dict,query_products_dict

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description = "build query to Queries mapping")
	parser.add_argument('--query_viewed_products_file',  required=True, help="file containing query to viewed products")
	parser.add_argument('--query_less_results_file',  required=True, help="file containing query to missing products")
	parser.add_argument('--qd_file',  required=True, help="file containing query to missing products")
	parser.add_argument('--output_file',  required=True, help="file containing query mappings")
	args = parser.parse_args()
	main(args.query_viewed_products_file,args.query_less_results_file,args.qd_file,args.output_file)

