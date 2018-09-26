#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "urgent_contact_report.json"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")
INDEX = "urgent_contact_report"
TYPE = "credit_data"
BODY_QUEUE = []

do_each_row = Proc.new do |fin, line|
	output_hash = Hash.new
	line.chomp!
	input_hash = JSON.parse(line)
	output_hash["old_id"] = input_hash["_id"]
#忽略身份证号不存在记录
	next unless output_hash["old_id"].kind_of? String
#获取report_id
	output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
#开始contactDetail
	output_hash["contactDetail"] = Hash.new
		if input_hash["l_report_base_contract"].is_a?(Array)
		output_hash["contactDetail"] = Array.new
		input_hash["l_report_base_contract"].each do |item|
			temp_hash = Hash.new
			temp_hash["contact_tel"] = item["contact_tel"]
			temp_hash["contact_name"] = item["contact_name"]
			temp_hash["contact_type"] = item["contact_type"]
			temp_hash["call_len"] = item["n_call_cnt"]
			temp_hash["sms_cnt"] = item["n_sms_cnt"]
			temp_hash["begin_date"] = item["c_begin_date"]
			temp_hash["end_date"] = item["c_end_date"]
			temp_hash["total_count"] = item["n_total_count"]
			temp_hash["total_amount"] = item["n_total_amount"]
			output_hash["contactDetail"] << temp_hash
		end
	end
#写入es
	out_body = gen_store_doc_bodies(gen_id_body(INDEX, TYPE, output_hash["report_id"],output_hash),  BODY_QUEUE, 2000)
	ES_DB.bulk_push(out_body) if out_body.is_a? Array
end


File.open(file_input, "r") do |fin|
	fin.each do |line|
		do_each_row.call(fin, line)
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end
