#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'

file_input = "/tmp/urgent_contact_report_history.json"
file_output = "/tmp/urgent_contact_report_history_out.json"
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
			output_hash["contactDetail"] = Array.new
			transfer_list(input_hash["l_base_info_history"],
				output_hash["contactDetail"], "l_contacts")
			#写入json
			fout.puts(output_hash.to_json)
		end
	end
end
