#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "shebao_info.json"
INDEX = "shebao_info"
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
		# 忽略社保记录为空记录
		next unless input_hash["c_shebao_info"] and input_hash["c_shebao_info"].size > 0
		output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
		output_hash["shebao_data"] = input_hash["c_shebao_info"].to_json
		# 增加字段识别jjd和第一风控
		out_hash["system_name"] = "JJD"
		#写入es
		out_body = gen_store_doc_bodies(gen_id_body(INDEX, TYPE, output_hash["report_id"],output_hash),  BODY_QUEUE, 1000)
		ES_DB.bulk_push(out_body) if out_body.is_a? Array
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end
