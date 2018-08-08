#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require './sqlite_treat'
require './common_funcs'
require 'json'
require './es_handler'

Encoding.default_external=Encoding.find("utf-8")

file_input = "/tmp/t_face_verify.txt"
INDEX = "test_face_verify"
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
		temp["live_img"] = row[1]
		temp["live_status"] = row[2]
		temp["card_front_img"] = row[3]
		temp["card_back_img"] = row[4]
		temp["card_verify_status"] = row[5]
		temp["compare_status"] = row[6]
		temp["update_time"] = date2int(row[7])
		temp["crt_tm"] = date2int(row[8])
		temp["biz_id"] = row[9]
		temp["n_type"] = row[10]
		temp["name"] = row[11]
		temp["idcard_no"] = row[12]
		temp["biz_no"] = row[13]
		out_body = gen_store_doc_bodies(INDEX, TYPE, temp, BODY_QUEUE, 3000)
		ES_DB.bulk_push(out_body) if out_body.is_a? Array
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(INDEX, TYPE, BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end
