#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "/tmp/EbusinessContact.json"
INDEX = "ebusiness_contact_report"
TYPE = "credit_data"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")
BODY_QUEUE = []

do_each_row = Proc.new do |fin,line|
	output_hash = Hash.new
	line.chomp!
	input_hash = JSON.parse(line)
	output_hash["old_id"] = input_hash["_id"]
#忽略身份证号不存在记录
	next unless output_hash["old_id"].kind_of? String
#忽略contact为空的记录
	next unless input_hash["l_report_collection_contact"] && input_hash["l_report_collection_contact"].size > 0
#获取report_id
	output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
	output_hash["update_time"] = Time.now.to_i
#开始deliver_addresse_list
	output_hash["l_report_collection_contact"] = Array.new
	input_hash["l_report_collection_contact"].each do |item|
		temp_hash = Hash.new
#获取多个名字相同字段
		%w{contact_type contact_name begin_date end_date total_coun total_amount t_begin_date t_end_date}.each do |key|
			temp_hash[key] = item[key]
		end
#兼容可能的时间对象
		temp_hash["t_begin_date"] = date_compat(temp_hash["t_begin_date"]) if temp_hash["t_begin_date"]
		temp_hash["t_end_date"] = date_compat(temp_hash["t_end_date"]) if temp_hash["t_end_date"]
		temp_hash["begin_date"] = date_compat(temp_hash["begin_date"]) if temp_hash["begin_date"]
		temp_hash["end_date"] = date_compat(temp_hash["end_date"]) if temp_hash["end_date"]
#处理contacts
		if item["contact_details"] && (item["contact_details"].is_a? Array)
			temp_hash["mobile_call_list"] = Array.new
			item["contact_details"].each do |contact|
				temp_contacts = Hash.new
				%w{phone_num call_cnt call_len call_out_cnt call_out_len call_in_cnt call_in_len sms_cnt}.each do |k|
					temp_contacts[k] = contact[k]
				end
				temp_hash["mobile_call_list"] << temp_contacts
			end
		end
		temp_hash["update_time"] = Time.now.to_i
		output_hash["l_report_collection_contact"] << temp_hash
	end

#写入es
	out_body = gen_store_doc_bodies(INDEX, TYPE, output_hash, BODY_QUEUE, 1000)
	ES_DB.bulk_push(out_body) if out_body.is_a? Array
end


File.open(file_input, "r") do |fin|
	fin.each do |line|
		do_each_row.call(fin, line)
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(INDEX, TYPE, BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end
