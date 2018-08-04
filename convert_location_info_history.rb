#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "/tmp/location_info_history.json"
INDEX = "test_location_info_history"
TYPE = "history"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("192.168.30.209:9200", "192.168.30.207:9200", "192.168.30.208:9200")

File.open(file_input, "r") do |fin|
	fin.each do |line|
		output_hash = Hash.new
		line.chomp!
		input_hash = JSON.parse(line)
		output_hash["old_id"] = input_hash["_id"]
	#忽略身份证号不存在记录
		next unless output_hash["old_id"].kind_of? String
		output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
		output_hash["_id"] = output_hash["report_id"]
		if input_hash["l_user_locations"].is_a?(Array)
			output_hash["location_info_list"] = Array.new
			input_hash["l_user_locations"].each do |item|
				temp_hash = Hash.new
				temp_hash["province_name"] = item["c_location_province"]
				temp_hash["city_name"] = item["c_location_city"]
				temp_hash["district_name"] = item["c_location_district"]
				temp_hash["address"] = item["c_location_address"]
				temp_hash["location_tm"] = date_compat(item["t_location_tm"])
				temp_hash["update_time"] = Time.now.to_s
				output_hash["location_info_list"] << temp_hash
			end
		end
		output_hash["update_time"] = Time.now.to_i
		#写入es
		ES_DB.store(INDEX, TYPE, output_hash)
	end
end
