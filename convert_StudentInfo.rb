#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './mongo_handler'

file_input = "/tmp/StudentInfo.json"
file_output = "/tmp/StudentInfo_out.json"
SQLDB = MyDB.new("ids.db", "id_pairs")
MONDB = Deal_Mongo.new("10.25.141.106:18000", "credit", "c_user_data", "trans", "123456")

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
#查询l_xueji_info列表
	xueji_info = MONDB.fetch_item_from_id(output_hash["old_id"])["l_xueji_info"] rescue nil
#开始student_info_list
	if input_hash["l_report_xuexin"].is_a?(Array)
		output_hash["student_info_list"] = Array.new
		for i in (0 ... input_hash["l_report_xuexin"].size)
			temp_hash = Hash.new
#获取多个字段
			%w{c_enter_img c_graduate_img c_university c_major c_student_begin_time c_student_end_time c_student_level c_student_status c_full_time}.each do |key|
				temp_hash[key] = input_hash["l_report_xuexin"][i][key]
			end
#添加l_xueji_info
			if xueji_info.is_a?(Array)
				temp_hash["name"] = xueji_info[i]["name"]
				temp_hash["idCardNo"] = xueji_info[i]["idCardNo"]
			end
			output_hash["student_info_list"] << temp_hash
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
