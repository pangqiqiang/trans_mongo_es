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
INDEX = "test_mobile_bill_report"
TYPE = "history"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("192.168.30.209:9200", "192.168.30.207:9200", "192.168.30.208:9200")


File.open(file_input, "r") do |fin|
	fin.each do |line|
		output_hash = Hash.new
		line.chomp!
		input_hash = JSON.parse(line)
		#忽略身份证号不存在记录
		next unless output_hash["old_id"].kind_of? String
		output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
		output_hash["_id"] = output_hash["report_id"]
		output_hash["_id"] = output_hash["report_id"]
		output_hash["report_bill_list"] = input_hash["l_mobile_bill"]
		output_hash["update_time"] = Time.now.to_i
#写入es
		ES_DB.store(INDEX, TYPE, output_hash)
	end
end
