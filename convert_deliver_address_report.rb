#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "/tmp/deliver_address_report.json"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("192.168.30.209:9200", "192.168.30.207:9200", "192.168.30.208:9200")
INDEX = "test_deliver_address_report"
TYPE = "credit_data"
BODY_QUEUE = []

do_each_row = Proc.new do |fin, line|
	output_hash = Hash.new
	line.chomp!
	input_hash = JSON.parse(line)
	output_hash["old_id"] = input_hash["_id"]
#忽略身份证号不存在记录
	next unless output_hash["old_id"].kind_of? String
忽略deliver为空的记录
	next unless input_hash["l_report_deliver_address"] && input_hash["l_report_deliver_address"].size > 0
#获取report_id
	output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
	output_hash["update_time"] = Time.now.to_i
#开始deliver_addresse_list
	output_hash["deliver_addresse_list"] = Array.new
	input_hash["l_report_deliver_address"].each do |item|
		temp_hash = Hash.new
#获取多个名字相同字段
		%w{address lng lat begin_date total_amount total_count t_begin_date t_end_date predict_addr_type}.each do |key|
			temp_hash[key] = item[key]
		end
#兼容可能的时间对象
		temp_hash["t_begin_date"] = date_compat(temp_hash["t_begin_date"]) if temp_hash["t_begin_date"]
		temp_hash["t_end_date"] = date_compat(temp_hash["t_end_date"]) if temp_hash["t_end_date"]
#处理receiver
		if item["receiver"] && (item["receiver"].is_a? Array)
			temp_hash["ebusiness_receiver_list"] = Array.new
			item["receiver"].each do |receiver|
				temp_recervers = Hash.new
				%w{amout count name phone_num_list}.each do |k|
					temp_recervers[k] = receiver[k]
				end
				temp_hash["ebusiness_receiver_list"] << temp_recervers
			end
		end
		output_hash["deliver_addresse_list"] << temp_hash
	end
#写入es
	out_body = gen_store_doc_bodies(INDEX, TYPE, output_hash, BODY_QUEUE, 2000)
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
