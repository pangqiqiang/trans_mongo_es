#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "/tmp/gjj_info.json"
INDEX = "test_gjj_info"
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
		output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
		output_hash["gjj_data"] = input_hash["c_gjj_info"].to_s
		out_body = gen_store_doc_bodies(INDEX, TYPE, output_hash, BODY_QUEUE, 3000)
		ES_DB.bulk_push(out_body) if out_body.is_a? Array
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(INDEX, TYPE, BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end
