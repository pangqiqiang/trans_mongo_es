#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "/tmp/urgent_contact_report_history.json"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("192.168.30.209:9200", "192.168.30.207:9200", "192.168.30.208:9200")
INDEX = "test_urgent_contact_report_history"
TYPE = "credit_data"
BODY_QUEUE = []

File.open(file_input, "r") do |fin|
	fin.each do |line|
		output_hash = Hash.new
		line.chomp!
		input_hash = JSON.parse(line)
		output_hash["old_id"] = input_hash["_id"]
	#忽略身份证号不存在记录
		next unless output_hash["old_id"].kind_of? String
		output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
		output_hash["contactDetail"] = Array.new
		transfer_list(input_hash["l_base_info_history"],
			output_hash["contactDetail"], "l_contacts")
#写入es
	out_body = gen_store_doc_bodies(INDEX, TYPE, output_hash, BODY_QUEUE, 3000)
	ES_DB.bulk_push(out_body) if out_body.is_a? Array
	end
end
