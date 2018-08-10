#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "/tmp/EbusinessExpense.json"
INDEX = "test_ebusiness_expense_report"
TYPE = "credit_data"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("192.168.30.209:9200", "192.168.30.207:9200", "192.168.30.208:9200")
BODY_QUEUE = []

File.open(file_input, "r") do |fin|
	fin.each do |line|
		output_hash = Hash.new
		line.chomp!
		input_hash = JSON.parse(line)
		output_hash["old_id"] = input_hash["_id"]
		#忽略身份证号不存在记录
		next unless output_hash["old_id"].kind_of? String
		#忽略expanse不存在为空的记录
		next unless input_hash["l_report_ebusiness_expense"] && input_hash["l_report_ebusiness_expense"].size > 0
		output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
		output_hash["update_time"] = Time.now.to_i
		#开始ebusiness_expense_list
		output_hash["ebusiness_expense_list"] = Array.new
		input_hash["l_report_ebusiness_expense"].each do |item|
			temp_hash = Hash.new
			%w{trans_mth all_amount all_count category}.each do |key|
				temp_hash[key] = item[key]
			end
			output_hash["ebusiness_expense_list"] << temp_hash
		end
		#写入es
		out_body = gen_store_doc_bodies(INDEX, TYPE, output_hash, BODY_QUEUE, 1000)
		ES_DB.bulk_push(out_body) if out_body.is_a? Array
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(INDEX, TYPE, BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end
