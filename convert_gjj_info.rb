#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'

file_input = "/tmp/gjj_info.json"
file_output = "/tmp/gjj_info_out.json"
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
			output_hash["gjj_data"] = input_hash["c_gjj_info"]
			#写入json
			fout.puts(output_hash.to_json)
		end
	end
end
