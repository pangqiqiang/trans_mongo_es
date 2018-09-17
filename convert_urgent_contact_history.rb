#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "urgent_contact_report_history.json"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")
INDEX = "urgent_contact_report_history"
TYPE = "credit_data"
BODY_QUEUE = []

File.open(file_input, "r") do |fin|
	fin.each do |line|
		#output_hash = Hash.new
		line.chomp!
		input_hash = JSON.parse(line)
		old_id = input_hash["_id"]
	#忽略身份证号不存在记录
		next unless input_hash["_id"].kind_of? String
		user_report_id = SQLDB.fetch_from_id(old_id)
		next unless  input_hash["l_base_info_history"].is_a? Array and input_hash["l_base_info_history"].size > 0
		input_hash["l_base_info_history"].each_with_index do |item|
			#next unless  item.is_a? Array and item.size > 0
			output_hash = Hash.new
			output_hash["old_id"] = old_id
			output_hash["user_report_id"] = user_report_id
			output_hash["contactDetail"] = item["l_contacts"]
			output_hash["update_time"] = date2int(item["t_base_upd_tm"])
			#写入es
			out_body = gen_store_doc_bodies(gen_body(INDEX,TYPE,output_hash),  BODY_QUEUE, 2000)
			ES_DB.bulk_push(out_body) if out_body.is_a? Array
		end		
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end
