#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'

file_input = "/tmp/EbusinessContact.json"
file_output = "/tmp/EbusinessContact_out.json"
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
	output_hash["update_time"] = Time.now.to_i
#开始deliver_addresse_list
	if input_hash["l_report_collection_contact"] && (input_hash["l_report_collection_contact"].is_a? Array)
		output_hash["l_report_collection_contact"] = Array.new
		input_hash["l_report_collection_contact"].each do |item|
			temp_hash = Hash.new
#获取多个名字相同字段
			%w{contact_type contact_name begin_date end_date total_coun total_amount t_begin_date t_end_date}.each do |key|
				temp_hash[key] = item[key]
			end
#兼容可能的时间对象
			temp_hash["t_begin_date"] = date_compat(temp_hash["t_begin_date"]) if temp_hash["t_begin_date"]
			temp_hash["t_end_date"] = date_compat(temp_hash["t_end_date"]) if temp_hash["t_end_date"]
			temp_hash["begin_date"] = date_compat(temp_hash["begin_date"]) if temp_hash["begin_date"]
			temp_hash["end_date"] = date_compat(temp_hash["end_date"]) if temp_hash["end_date"]
#处理contacts
			if item["contact_details"] && (item["contact_details"].is_a? Array)
				temp_hash["mobile_call_list"] = Array.new
				item["contact_details"].each do |contact|
					temp_contacts = Hash.new
					%w{phone_num call_cnt call_len call_out_cnt call_out_len call_in_cnt call_in_len sms_cnt}.each do |k|
						temp_contacts[k] = contact[k]
					end
					temp_hash["mobile_call_list"] << temp_contacts
				end
			end
			temp_hash["update_time"] = Time.now.to_i
			output_hash["l_report_collection_contact"] << temp_hash
		end
	end

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
