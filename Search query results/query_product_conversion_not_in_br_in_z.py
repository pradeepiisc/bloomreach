import argparse
'''
s3://brp-mobile/coverage/redshift/staples/query_product_conversion_z
s3://brp-mobile/coverage/redshift/staples/query_product_conversion_br 

for merchant staples

UNLOAD('SELECT browse_session_query,browse_session_products_atc ,conversions from staples.browse_sessions
WHERE cdate between ''20151011'' and ''20151018''
AND   browse_session_type != ''(not set)''
AND   (snap_experiment = ''T'')
AND browse_session_products_atc != ''(not set)''
AND conversions > 0 
') to 's3://brp-mobile/coverage/redshift/staples/query_product_conversion_br/'
credentials 'aws_access_key_id=AKIAIBS6WJXTFANJPDYA;aws_secret_access_key=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' delimiter as '\t' allowoverwrite ;

'''
def main(bloomreach_query_product_conversion_file,z_query_product_conversion_file,output_file):
	qp_conversion_dict_br = get_query_products(bloomreach_query_product_conversion_file)
	print qp_conversion_dict_br
	print 'br file has been read'
	qp_conversion_dict_z  = get_query_products(z_query_product_conversion_file)
	print 'zettata file has been 	read'
	query_product_set_diff = get_diff_from_dict(qp_conversion_dict_z,qp_conversion_dict_br)
	write_to_file(query_product_set_diff,output_file)

def get_query_products(query_product_file):
	qp_file_handler = open(query_product_file)
	qp_conversion_dict = {}
	for line in qp_file_handler:
		line_contents = line.strip().split('\t')
		query = line_contents[0]
		products = line_contents[1].split(',')
		#print query,products
		if len(products) > 1:
			for each_product in products:
				qp_set.add((query,each_product))
		else:
			qp_conversion_dict[(query,products[0])] = qp_conversion_dict.get((query,products[0]),0)	
			qp_conversion_dict[(query,products[0])] += int(line_contents[-1])

	qp_file_handler.close()
	return qp_conversion_dict

def get_diff_from_dict(dict1,dict2):
	keys1 = dict1.keys()
	keys2 = dict2.keys()
	return keys1.difference(keys2)

def write_to_file(query_product_set_diff,output_file):
	file_handler = open(output_file,'w')
	for qp_tuples in query_product_set_diff:
		file_handler.write(qp_tuples[0] + '\t'+ qp_tuples[1]+ '\n')

	file_handler.close()

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = "return file containing query product pairs which were converted for zettata but not BR")
  parser.add_argument('--bloomreach_file',  required=True, help="file containing query product pair conversions for BR")
  parser.add_argument('--z_file',  required=True, help="file containing query product pair conversions for Z")
  parser.add_argument('--output_file',  required=True, help="tsv file containing query product pair which were converted for Z but not BR")
  args = parser.parse_args()
  args = parser.parse_args()
  main(args.bloomreach_file,args.z_file,args.output_file)

