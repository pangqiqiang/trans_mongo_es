#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require './sqlite_treat'
require './common_funcs'
require 'json'
require './es_handler'
Encoding.default_external=Encoding.find("utf-8")

file_input = "/tmp/t_face_verify_result.txt"
INDEX = "test_face_verify_result"
TYPE = "credit_data"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("192.168.30.209:9200", "192.168.30.207:9200", "192.168.30.208:9200")
BODY_QUEUE = []


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
		out_body = gen_store_doc_bodies(INDEX, TYPE, temp, BODY_QUEUE, 1000)
		ES_DB.bulk_push(out_body) if out_body.is_a? Array
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(INDEX, TYPE, BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end


