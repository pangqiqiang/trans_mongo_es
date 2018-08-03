#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
file_input = "/tmp/person_report.json"
file_output = "/tmp/person_report_out.json"
SQLDB = MyDB.new("ids.db", "id_pairs")

do_each_row = Proc.new do |fin, fout, line|
	output_hash = Hash.new
	line.chomp!
	input_hash = JSON.parse(line)
	output_hash["old_id"] = input_hash["_id"]
#忽略身份证号不存在记录
	next unless output_hash["old_id"].kind_of? String
#获取report_id
	output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
	output_hash["_id"] = output_hash["report_id"]
#开始contactDetail
	output_hash["contactDetail"] = Hash.new
	rename_hash_item(input_hash["l_report_base_contract"],output_hash["contactDetail"],
	 %w<contact_tel contact_name contact_type>)
	output_hash["contactDetail"]["call_cnt"] = hash_link(output_hash,
		["l_report_base_contract", "n_call_cnt"])
	output_hash["contactDetail"]["call_len"] = hash_link(output_hash,
		["l_report_base_contract", "n_call_len"])
	output_hash["contactDetail"]["sms_cnt"] = hash_link(output_hash,
		["l_report_base_contract", "n_sms_cnt"])
	output_hash["contactDetail"]["begin_date"] = hash_link(output_hash,
		["l_report_base_contract", "c_begin_dat"])
	output_hash["contactDetail"]["end_date"] = hash_link(output_hash,
		["l_report_base_contract", "end_date"])
	output_hash["contactDetail"]["total_count"] = hash_link(output_hash,
		["l_report_base_contract", "n_total_count"])
	output_hash["contactDetail"]["total_amount"] = hash_link(output_hash,
		["l_report_base_contract", "n_total_amount"])
#写入json
	fout.puts(output_hash.to_json)
end


File.open(file_input, "r") do |fin|
	File.open(file_output, "w") do |fout|
		fin.each do |line|
			do_each_row.call(fin, fout, line)
		end
	end
end
