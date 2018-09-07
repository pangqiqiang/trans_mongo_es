#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
require './es_handler'
require './mysql_deal'
require 'thread'

INDEX = "user_info"
TYPE = "credit_data"
SQLDB = MyDB.new("ids.db", "id_pairs")
MSQLDB = Mysql_DB.new("10.111.30.20", 3306, "op", "KRkFcVCbopZbS8R7", "jjd_11th",  "user_passport")
SQLDB.create_index
ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")
body_queue0=[]; body_queue1=[]; body_queue2=[]; body_queue3=[]; body_queue4=[]; body_queue5=[]
body_queue6=[]; body_queue7=[];body_queue8=[]; body_queue9=[]
$queue = Queue.new
#防止线程无警告中断
Thread.abort_on_exception = true

do_each_row = Proc.new do |line, body_queue, sql_queue|
	output_hash = Hash.new
	line.chomp!
	input_hash = JSON.parse(line)
	output_hash["old_id"] = input_hash["_id"]
#忽略身份证号不存在记录
	next unless output_hash["old_id"].kind_of? String
	#插入id获取es主键_id值
	report_id = ES_DB.store(INDEX, TYPE, output_hash)
	output_hash["report_id"] = report_id
	output_hash["puid"] = hash_link(input_hash, ["l_business_system", 0, "_id"])
	output_hash["ouid"] = output_hash["puid"]
	#根据puid获取新id
	output_hash["puid"] = MSQLDB.get_from_salt(output_hash["ouid"]) 

#维护report_id, id, uid映射关系
#100条一次事务加快速度
	#sql_bulk = gen_sql_list([output_hash["old_id"],report_id,output_hash["puid"]], SQL_VALUES, 100)
	#SQLDB.store(output_hash["old_id"], report_id, output_hash["puid"])
	sql_queue << [output_hash["old_id"], report_id, output_hash["ouid"]]
	#output_hash["quid"] = hash_link(input_hash, ["l_business_system", 0, "_id"])
	output_hash["system_type"] = hash_link(input_hash, ["l_business_system", 0, "c_system_name"])
	output_hash["user_name"] = hash_link(input_hash, ["c_base_info","c_user_name"])
	output_hash["card_no"] = input_hash["_id"]
	output_hash["telephone"] = hash_link(input_hash, ["l_business_system", 0, "c_telephone"])
	output_hash["data_change"] = hash_link(input_hash, ["c_base_info","b_data_change"])
	output_hash["student_status"] = -1; output_hash["bind_card"] = true
	output_hash["up_special"] = hash_link(input_hash, ["c_base_info","b_xueli_up_zhuanke"])
	output_hash["mobile_phone"] = hash_link(input_hash, ["c_base_info","c_mobile_phone"])
	output_hash["base_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_base_upd_tm"]))
	output_hash["mobile_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_mobile_upd_tm"]))
	output_hash["taobao_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_taobao_upd_tm"]))
	output_hash["jingdong_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_jingdong_upd_tm"]))
	output_hash["shebao_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_shebao_upd_tm"]))
	output_hash["gjj_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_gjj_upd_tm"]))
   	output_hash["xuexin_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_xuexin_upd_tm"]))
   	output_hash["zhengxin_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_zhengxin_upd_tm"]))
   	output_hash["house_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_house_upd_tm"]))
   	output_hash["car_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_car_upd_tm"]))
   	output_hash["income_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_income_upd_tm"]))
   	output_hash["job_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_job_upd_tm"]))
   	output_hash["zhima_credit_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_zhima_credit_upd_tm"]))
	output_hash["mobile_analysis_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_mobile_analysis_upd_tm"]))
	output_hash["location_upd_tm"] = date2int(hash_link(input_hash, ["c_base_info","t_location_upd_tm"]))
	output_hash["baseInfo_credit_status"] = bool2int(hash_link(input_hash,["c_base_info","b_base_info"]),
		output_hash["base_upd_tm"])
	output_hash["mobile_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_mobile_info"]),
		output_hash["mobile_upd_tm"])
	output_hash["taobao_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_taobao_info"]),
		output_hash["taobao_upd_tm"])
	output_hash["jingdong_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_jingdong_info"]),
		output_hash["jingdong_upd_tm"])
	output_hash["shebao_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_shebao_info"]),
		output_hash["shebao_upd_tm"])
	output_hash["gjj_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_gjj_info"]),
		output_hash["gjj_upd_tm"])
	output_hash["xuexin_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_xuexin_info"]),
		output_hash["xuexin_upd_tm"])
	output_hash["zhengxin_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_zhengxin_info"]),
		output_hash["zhengxin_upd_tm"])
	output_hash["house_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_house_info"]),
		output_hash["house_upd_tm"])
	output_hash["car_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_car_info"]),
		output_hash["car_upd_tm"])
	output_hash["income_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_income_info"]),
		output_hash["income_upd_tm"])
	output_hash["job_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_job_info"]),
		output_hash["job_upd_tm"])
	output_hash["zhima_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_zhima_credit"]),
		output_hash["zhima_credit_upd_tm"])
	output_hash["mobileAnalysis_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_mobile_analysis_info"]),
		output_hash["mobile_analysis_upd_tm"])
	output_hash["location_credit_status"] = bool2int(hash_link(input_hash, ["c_base_info","b_location_info"]),
		output_hash["location_upd_tm"])
	output_hash["update_time"] = Time.now.to_i
	out_body = gen_update_doc_bodies(INDEX, TYPE, output_hash, body_queue, "report_id", 3000)
	ES_DB.bulk_push(out_body) if out_body.is_a? Array
end

def thr_gen(filename, do_each_row, body_queue, sql_queue)
	File.open(filename, "r") do |fin|
		fin.each do |line|
			do_each_row.call(line, body_queue, sql_queue)
		end
#处理es最后未到limit的记录
		if body_queue.size > 0
			out_body = gen_remain_update_bodies(INDEX, TYPE, body_queue, "report_id")
			ES_DB.bulk_push(out_body)
		end
	end
end

Thread.new {thr_gen("/tmp/user_info_000", do_each_row, body_queue0, $queue)}
Thread.new {thr_gen("/tmp/user_info_001", do_each_row, body_queue1, $queue)}
Thread.new {thr_gen("/tmp/user_info_002", do_each_row, body_queue2, $queue)}
Thread.new {thr_gen("/tmp/user_info_003", do_each_row, body_queue3, $queue)}
Thread.new {thr_gen("/tmp/user_info_004", do_each_row, body_queue4, $queue)}
Thread.new {thr_gen("/tmp/user_info_005", do_each_row, body_queue5, $queue)}
Thread.new {thr_gen("/tmp/user_info_006", do_each_row, body_queue6, $queue)}
Thread.new {thr_gen("/tmp/user_info_007", do_each_row, body_queue7, $queue)}
Thread.new {thr_gen("/tmp/user_info_006", do_each_row, body_queue8, $queue)}
Thread.new {thr_gen("/tmp/user_info_007", do_each_row, body_queue9, $queue)}

#生产速度慢，先跑30s
sleep 30

consumer = Thread.new do
	until $queue.empty?
		#如果队列少于5，停止30s等等生产,防止丢失数据
		sleep 30 if $queue.size <= 5
		value = $queue.pop
		#p value
		SQLDB.store(value)
	end
end

consumer.join
