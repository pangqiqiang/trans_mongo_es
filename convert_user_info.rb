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
DB_PASS = {host:"rm-2zeoc1o61ykfe62v6.mysql.rds.aliyuncs.com", port:3306, user:"dev", 
			pass:"KRkFcVCbopZbS8R7", database:"jjd", table: "user_passport"}
DB_FACE = {host:"10.111.33.181", port:3306, user:"mysqltords", pass:"TeNSXaGXbMz8eY86", database:"jjd", table: "t_face_verify"}

MSQLDB0 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB0 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])
MSQLDB1 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB1 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])
MSQLDB2 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB2 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])
MSQLDB3 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB3 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])
MSQLDB4 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB4 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])
MSQLDB5 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB5 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])
MSQLDB6 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB6 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])
MSQLDB7 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB7 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])
MSQLDB8 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB8 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])
MSQLDB9 = Mysql_DB.new(DB_PASS[:host], DB_PASS[:port], DB_PASS[:user], DB_PASS[:pass], DB_PASS[:database], DB_PASS[:table])
FACEDB9 = Mysql_DB.new(DB_FACE[:host], DB_FACE[:port], DB_FACE[:user], DB_FACE[:pass], DB_FACE[:database], DB_FACE[:table])

SQLDB.create_index
ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")
body_queue0=[]; body_queue1=[]; body_queue2=[]; body_queue3=[]; body_queue4=[]; body_queue5=[]
body_queue6=[]; body_queue7=[];body_queue8=[]; body_queue9=[]
$queue = Queue.new
#防止线程无警告中断
Thread.abort_on_exception = true

do_each_row = Proc.new do |line, body_queue, sql_queue,face_db,user_db|
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
	output_hash["puid"] = user_db.get_from_salt(output_hash["ouid"]).to_s
	#获取face_verify_status,face_verify_upd_tm
	face_status, face_tm = face_db.get_face_from_uid(output_hash["ouid"])
	case face_status
	when 0
		output_hash["face_verify_status"] = 4
	when 1
		output_hash["face_verify_status"] = 3
	else
		output_hash["face_verify_status"] = 0
	end
	output_hash["face_verify_upd_tm"] = date2int(face_tm)
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
	output_hash["data_change"] = true
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
	# 增加字段识别jjd和第一风控
	output_hash["system_name"] = "JJD"
	
	out_body = gen_update_doc_bodies(INDEX, TYPE, output_hash, body_queue, "report_id", 3000)
	ES_DB.bulk_push(out_body) if out_body.is_a? Array
end

def thr_gen(filename, do_each_row, body_queue, sql_queue,face_db,user_db)
	File.open(filename, "r") do |fin|
		fin.each do |line|
			do_each_row.call(line, body_queue, sql_queue, face_db, user_db)
		end
#处理es最后未到limit的记录
		if body_queue.size > 0
			out_body = gen_remain_update_bodies(INDEX, TYPE, body_queue, "report_id")
			ES_DB.bulk_push(out_body)
		end
	end
end

Thread.new {thr_gen("user_info_000", do_each_row, body_queue0, $queue,FACEDB0, MSQLDB0)}
Thread.new {thr_gen("user_info_001", do_each_row, body_queue1, $queue,FACEDB1, MSQLDB1)}
Thread.new {thr_gen("user_info_002", do_each_row, body_queue2, $queue,FACEDB2, MSQLDB2)}
Thread.new {thr_gen("user_info_003", do_each_row, body_queue3, $queue,FACEDB3, MSQLDB3)}
Thread.new {thr_gen("user_info_004", do_each_row, body_queue4, $queue,FACEDB4, MSQLDB4)}
Thread.new {thr_gen("user_info_005", do_each_row, body_queue5, $queue,FACEDB5, MSQLDB5)}
Thread.new {thr_gen("user_info_006", do_each_row, body_queue6, $queue,FACEDB6, MSQLDB6)}
Thread.new {thr_gen("user_info_007", do_each_row, body_queue7, $queue,FACEDB7, MSQLDB7)}
Thread.new {thr_gen("user_info_008", do_each_row, body_queue8, $queue,FACEDB8, MSQLDB8)}
Thread.new {thr_gen("user_info_009", do_each_row, body_queue9, $queue,FACEDB9, MSQLDB9)}

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
