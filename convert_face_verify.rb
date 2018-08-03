#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require './sqlite_treat'
require './common_funcs'
require 'json'

Encoding.default_external=Encoding.find("utf-8")

file_input = "t_face_verify.txt"
file_output = "t_face_verify_out.json"
SQLDB = MyDB.new("ids.db", "id_pairs")


File.open(file_output, 'w') do |fout|
	File.open(file_input) do |fin|
		fin.each do |line|
			next if fin.lineno == 1
			temp = {}
			line.chomp!
			row = line.split("\t")
			temp["old_id"] = row[0]
			temp["report_id"] = SQLDB.fetch_from_uid(temp["old_id"])
			temp["live_img"] = row[1]
			temp["live_status"] = row[2]
			temp["card_front_img"] = row[3]
			temp["card_back_img"] = row[4]
			temp["card_verify_status"] = row[5]
			temp["compare_status"] = row[6]
			temp["update_tm"] = date2int(row[7])
			temp["crt_tm"] = date2int(row[8])
			temp["biz_id"] = row[9]
			temp["n_type"] = row[10]
			temp["name"] = row[11]
			temp["idcard_no"] = row[12]
			temp["biz_no"] = row[13]
			fout.puts(temp.to_json)
		end
	end
end
