#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require './sqlite_treat'
require './common_funcs'
require 'json'

file_input = "/tmp/t_face_verify_result.txt"
file_output = "/tmp/face_verify_result_out.json"
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
			temp["live_result"] = row[1]
			temp["card_verify_result"] = row[2]
			temp["compare_result"] = row[3]
			fout.puts(temp.to_json)
		end
	end
end


