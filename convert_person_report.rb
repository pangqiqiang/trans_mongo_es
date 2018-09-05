#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'

file_input = "/tmp/person_report.json"
INDEX = "person_report"
TYPE = "credit_data"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")
BODY_QUEUE = []

do_each_row = Proc.new do |fin, line|
	output_hash = Hash.new
	line.chomp!
	input_hash = JSON.parse(line)
	output_hash["old_id"] = input_hash["_id"]
#忽略身份证号不存在记录
	next unless output_hash["old_id"].kind_of? String
#获取report_id
	output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
	output_hash["real_name"] = hash_link(input_hash, ["c_report_person", "phone_num"])
	output_hash["home_addr"] = hash_link(input_hash, ["c_report_person", "c_home_addr"])
	output_hash["wechat_id"] = hash_link(input_hash, ["c_report_person", "c_wechat_id"])
	rename_hash_item(input_hash["c_report_person"], output_hash,
		%w<id_card_num gender age head>)

#开始zhengXinReport
	output_hash["zhengXinReport"] = Hash.new
	output_hash["zhengXinReport"]["courtAccept"] = hash_link(input_hash,
		["c_report_person", "n_court_accept"])
	output_hash["zhengXinReport"]["tongdunAccept"] = hash_link(input_hash,
		["c_report_person", "n_tongdun_accept"])
	output_hash["zhengXinReport"]["nameEqualsZhengxin"] = hash_link(input_hash,
		["c_report_person", "n_name_equals_zhengxin"])
	output_hash["zhengXinReport"]["zhengxinOverudeCounts_90day"] = hash_link(input_hash,
	 ["c_report_person", "n_zhengxin_overude_90_days"])
	output_hash["zhengXinReport"]["nameEqualsXueXin"] = hash_link(input_hash,
	 ["c_report_person", "n_name_equals_xuexin"])
	output_hash["zhengXinReport"]["idcardEqualsXueXin"] = hash_link(input_hash,
	 ["c_report_person", "n_idcard_equals_xuexin"])
	output_hash["zhengXinReport"]["zhengxinAccountCount"] = hash_link(input_hash,
	 ["c_report_person", "n_zhengxin_account_count"])
	output_hash["zhengXinReport"]["zhengxinHouseCount"] = hash_link(input_hash,
	 ["c_report_person", "n_zhengxin_house_count"])
	output_hash["zhengXinReport"]["zhengxinOtherCount"] = hash_link(input_hash,
	 ["c_report_person", "n_zhengxin_other_count"])
	output_hash["zhengXinReport"]["zhengxinOverdueCount"] = hash_link(input_hash,
	 ["c_report_person", "n_zhengxin_overdue_count"])
	rename_hash_item(input_hash["c_report_person"], output_hash["zhengXinReport"],
		%w<watchListDetails antifraudScore antifraudVerify antifraudRisk>)
	output_hash["zhengXinReport"]["antifraudVerify"] = output_hash["zhengXinReport"]["antifraudVerify"].to_json
	output_hash["zhengXinReport"]["antifraudRisk"] = output_hash["zhengXinReport"]["antifraudRisk"].to_json
#开始ebusinessReport
	output_hash["ebusinessReport"] = Hash.new
	output_hash["ebusinessReport"]["ebusynessTotalAmount"] = hash_link(input_hash,
	 ["c_report_person", "n_ebusyness_total_amount"])
	output_hash["ebusinessReport"]["ebusynessTotalTm"] = hash_link(input_hash,
	 ["c_report_person", "n_ebusyness_total_tm"])
	output_hash["ebusinessReport"]["ebusynessTotalCount"] = hash_link(input_hash,
	 ["c_report_person", "n_ebusyness_total_count"])
	output_hash["ebusinessReport"]["telEbusynessCount"] = hash_link(input_hash,
	 ["c_report_person", "n_tel_ebusyness_count"])
#开始mobileReport
	output_hash["mobileReport"] = Hash.new
	output_hash["mobileReport"]["mobilePhone"] = hash_link(input_hash,
	 ["c_report_person", "c_mobile_phone"])
	output_hash["mobileReport"]["telUseTm"] = hash_link(input_hash,
	 ["c_report_person", "n_tel_use_tm"])
	output_hash["mobileReport"]["telExchange"] = hash_link(input_hash,
	 ["c_report_person", "n_tel_exchange"])
#写入es
	out_body = gen_store_doc_bodies(INDEX, TYPE, output_hash, BODY_QUEUE, 1000)
	ES_DB.bulk_push(out_body) if out_body.is_a? Array
end

File.open(file_input, "r") do |fin|
	fin.each do |line|
		do_each_row.call(fin, line)
	end
	if BODY_QUEUE.size > 0
		out_body = gen_remain_store_bodies(INDEX, TYPE, BODY_QUEUE)
		ES_DB.bulk_push(out_body)
	end
end
