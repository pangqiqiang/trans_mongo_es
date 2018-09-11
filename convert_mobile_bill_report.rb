#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './mongo_handler'
require './sqlite_treat'
require './es_handler'

file_input = "/tmp/mobile_bill_report.json"
INDEX = "mobile_bill_report"
TYPE = "credit_data"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")
BODY_QUEUE = []

File.open(file_input, "r") do |fin|
	fin.each do |line|
		output_hash = Hash.new
		line.chomp!
		input_hash = JSON.parse(line)
		output_hash["old_id"] = input_hash["_id"]
		#忽略身份证号不存在记录
		next unless output_hash["old_id"].kind_of? String
		#忽略bill字段为空记录
		next unless input_hash["l_mobile_bill"] and input_hash["l_mobile_bill"].size > 0
		output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
		output_hash["report_bill_list"] = input_hash["l_mobile_bill"]
		#total_amt字段修改为分自乘100，ruby多么简洁！！！！
		output_hash["report_bill_list"].map{|item| item["total_amt"] *=100 if item["total_amt"]}
		output_hash["update_time"] = Time.now.to_i
#写入es
		out_body = gen_store_doc_bodies(gen_id_body(INDEX, TYPE, output_hash["report_id"],output_hash),  BODY_QUEUE, 2000)
		ES_DB.bulk_push(out_body) if out_body.is_a? Array
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end
