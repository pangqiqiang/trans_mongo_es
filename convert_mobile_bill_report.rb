#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './mongo_handler'
require './sqlite_treat'

file_input = "/tmp/mobile_bill_report.json"
file_output = "/tmp/mobile_bill_report_out.json"
SQLDB = MyDB.new("ids.db", "id_pairs")


File.open(file_input, "r") do |fin|
	File.open(file_output, "w") do |fout|
		fin.each do |line|
			output_hash = Hash.new
			line.chomp!
			input_hash = JSON.parse(line)
			output_hash["old_id"] = input_hash["_id"]
			#忽略身份证号不存在记录
			next unless output_hash["old_id"].kind_of? String
			output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
			output_hash["_id"] = output_hash["report_id"]
			output_hash["_id"] = output_hash["report_id"]
			output_hash["report_bill_list"] = input_hash["l_mobile_bill"]
			output_hash["update_time"] = Time.now.to_i
			#写入json
			fout.puts(output_hash.to_json)
		end
	end
end
