#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'time'
require './common_funcs'
require './sqlite_treat'
require 'csv'

file_input = "/tmp/face_verify_result.csv"
file_output = "/tmp/face_verify_result_out.csv"
SQLDB = MyDB.new("ids.db", "id_pairs")


CSV.open(file_output, 'wb') do |fout|
	CSV.foreach(file_input) do |row|
		result_id = SQLDB.fetch_from_uid(row[0])
		row.insert(1, result_id, result_id)
		fout << row
	end
end
