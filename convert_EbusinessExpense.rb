#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'

file_input = "/tmp/EbusinessExpense.json"
file_output = "/tmp/EbusinessExpense_out.json"
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
			output_hash["update_time"] = Time.now.to_i
			if input_hash["l_report_ebusiness_expense"] && (input_hash["l_report_ebusiness_expense"].is_a? Array)
				output_hash["ebusiness_expense_list"] = Array.new
				input_hash["l_report_ebusiness_expense"].each do |item|
					temp_hash = Hash.new
					%w{trans_mth all_amount all_count category}.each do |key|
						temp_hash[key] = item[key]
					end
					output_hash["ebusiness_expense_list"] << temp_hash
				end
			end
			#写入json
			fout.puts(output_hash.to_json)
		end
	end
end
