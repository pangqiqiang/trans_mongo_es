#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './mongo_handler'
require './sqlite_treat'
require './es_handler'

file_input = "/tmp/report_basic_info.json"
DB = Deal_Mongo.new("10.25.141.106:18000", "credit", "c_user_data", "trans", "123456")
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("192.168.30.209:9200", "192.168.30.207:9200", "192.168.30.208:9200")
INDEX = "test_report_basic_info"
TYPE = "credit_data"
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
#开始user_base_info
	output_hash["user_base_info"] =  Hash.new
	output_hash["user_base_info"]["telephone"] = hash_link(input_hash, ["l_business_system", 0, "c_telephone"])
	rename_hash_item(input_hash["c_base_info"],output_hash["user_base_info"],
	 %w<level_1_name level_2_code level_2_name level_3_code level_3_name> )
	output_hash["user_base_info"]["home_addr"] = hash_link(input_hash,["c_base_info", "c_home_addr"])
	output_hash["user_base_info"]["wechat_id"] = hash_link(input_hash, ["c_base_info", "c_wechat_id"])
	output_hash["user_base_info"]["update_time"] = date2int(hash_link(input_hash, ["c_base_info","t_base_upd_tm"]))
##开始car_info
	output_hash["car_info"] =  Hash.new
	output_hash["car_info"]["car_brand"] = hash_link(input_hash, ["c_car_info","c_car_brand"])
	output_hash["car_info"]["car_mileage"] = hash_link(input_hash, ["c_car_info", "c_car_brand"])
	output_hash["car_info"]["car_price"] = hash_link(input_hash, ["c_car_info", "c_car_price"])
	output_hash["car_info"]["car_pay_status"] = hash_link(input_hash, ["c_car_info", "c_car_pay_status"])
	output_hash["car_info"]["car_paid"] = hash_link(input_hash, ["c_car_info", "c_car_paid"])
	rename_hash_item(input_hash["c_car_info"],output_hash["car_info"],
	 %w<level_1_code level_1_name level_2_code level_2_name level_3_code level_3_name>)
	output_hash["car_info"]["car_age"] = hash_link(input_hash, ["c_car_info", "c_car_age"])
	output_hash["car_info"]["car_is_used"] = hash_link(input_hash, ["c_car_info", "b_car_is_used"])
	output_hash["car_info"]["car_is_mortgage"] = hash_link(input_hash, ["c_car_info", "b_car_is_mortgage"])
	output_hash["car_info"]["car_image_list"] = hash_link(input_hash, ["c_car_info", "l_car_image"])
	output_hash["car_info"]["update_time"] = Time.now.to_i      #当前时间用整型表示
#开始earn_info
	output_hash["earn_info"] = Hash.new
	output_hash["earn_info"]["earn_month"] = hash_link(input_hash, ["c_earn_inf", "c_earn_month"])
	output_hash["earn_info"]["earn_image_list"] = hash_link(input_hash, ["c_earn_inf", "l_earn_image"])
	output_hash["earn_info"]["update_time"] = Time.now.to_i		#当前时间用整型表示
#开始house_info
	output_hash["house_info"] = Hash.new
	rename_hash_item(input_hash["c_house_info"],output_hash["house_info"],
	 %w<level_1_code level_1_name level_2_code level_2_name level_3_code level_3_name>)
	output_hash["house_info"]["house_address"] = hash_link(input_hash, ["c_house_info", "c_house_address"])
	output_hash["house_info"]["house_type"] = hash_link(input_hash, ["c_house_info", "c_house_type"])
	output_hash["house_info"]["house_area"] = hash_link(input_hash, ["c_house_info", "c_house_area"])
	output_hash["house_info"]["house_price"] = hash_link(input_hash, ["c_house_info", "c_house_price"])
	output_hash["house_info"]["house_pay_status"] = hash_link(input_hash, ["c_house_info", "c_house_pay_status"])
	output_hash["house_info"]["house_paid"] = hash_link(input_hash, ["c_house_info", "c_house_paid"])
	output_hash["house_info"]["house_age"] = hash_link(input_hash, ["c_house_info", "c_house_age"])
	output_hash["house_info"]["house_is_used"] = hash_link(input_hash, ["c_house_info", "b_house_is_used"])
	output_hash["house_info"]["house_is_mortgage"] = hash_link(input_hash, ["c_house_info", "b_house_is_mortgage"])
	output_hash["house_info"]["house_image_list"] = hash_link(input_hash, ["c_house_info", "l_house_image"])
	output_hash["house_info"]["update_time"] = Time.now.to_i
#开始job_info
	output_hash["job_info"] = Hash.new
	output_hash["job_info"]["company_name"] = hash_link(input_hash, ["c_job_info", "c_company"])
	output_hash["job_info"]["position"] = hash_link(input_hash, ["c_job_info", "c_position"])
	output_hash["job_info"]["employment_date"] = date2int(hash_link(input_hash, ["c_job_info", "c_employment_date"]))
	output_hash["job_info"]["company_tel"] = hash_link(input_hash, ["c_job_info", "c_company_tel"])
	rename_hash_item(input_hash["c_job_info"],output_hash["job_info"],
	 %w<level_1_code level_1_name level_2_code level_2_name level_3_code level_3_name> )
	output_hash["job_info"]["company_address"] = hash_link(input_hash, ["c_job_info", "company_address"])
	output_hash["job_info"]["company_image_list"] = hash_link(input_hash, ["c_job_info", "l_company_image"])
	output_hash["job_info"]["update_time"] = Time.now.to_i
#开始location_Info
	output_hash["location_Info"] = Hash.new
	output_hash["location_Info"]["province_name"] = hash_link(input_hash, ["c_recent_location_info", "c_location_recent_province"])
	output_hash["location_Info"]["city_name"] = hash_link(input_hash, ["c_recent_location_info", "c_location_recent_city"])
	output_hash["location_Info"]["district_name"] = hash_link(input_hash, ["c_recent_location_info", "c_location_recent_district"])
	output_hash["location_Info"]["address"] = hash_link(input_hash, ["c_recent_location_info", "c_location_recent_address"])
	output_hash["location_Info"]["location_tm"] = Time.now.to_i
	output_hash["location_Info"]["location_count"] = hash_link(input_hash, ["c_recent_location_info", "n_location_count"])
	output_hash["location_Info"]["update_time"] = date2int(hash_link(input_hash, ["c_recent_location_info", "t_location_recent_tm"]))
#开始gjj_base_info
	output_hash["gjj_base_info"] = Hash.new
	rename_hash_item(hash_link(input_hash, ["c_gjj_info", "task_data", "base_info"]),output_hash["gjj_base_info"],
	 %w<cert_type begin_date last_pay_date pay_status balance cust_no pay_status_desc cert_no corp_name name>)
#时间格式不统一做兼容处理,es
	output_hash["gjj_base_info"]["begin_date"] = date2int(output_hash["gjj_base_info"]["begin_date"])
	output_hash["gjj_base_info"]["last_pay_date"] = date2int(output_hash["gjj_base_info"]["last_pay_date"])


#开始shebao_base_info
	output_hash["shebao_base_info"] = Hash.new
	rename_hash_item(hash_link(input_hash, ["c_shebao_info", "task_data", "user_info"]), output_hash["shebao_base_info"],
	 %w<name nation gender hukou_type certificate_number home_address company_name mobile begin_date time_to_work>)
#时间格式不统一做兼容处理,es
	output_hash["shebao_base_info"]["begin_date"] = date_compat(output_hash["shebao_base_info"]["begin_date"])
	output_hash["shebao_base_info"]["time_to_work"] = date_compat(output_hash["shebao_base_info"]["time_to_work"])
#查询记录
	user_data = DB.fetch_item_from_id(output_hash["old_id"])
#开始ebusiness_basice_info
	output_hash["ebusiness_basice_info"] = Hash.new
	output_hash["ebusiness_basice_info"]["update_time"] = date2int(hash_link(user_data, ["c_jingdong_basic", "update_time"]))
	rename_hash_item(hash_link(user_data, ["c_jingdong_basic"]),
	output_hash["ebusiness_basice_info"],
	%w<level nickname real_name website_id email cell_phone>)
#开始mobile_info
	output_hash["mobile_basic_info"] = Hash.new
	rename_hash_item(hash_link(user_data, ["c_mobile_basic"]),
		output_hash["mobile_basic_info"],
	%w<cell_phone idcard reg_time real_name>)
	output_hash["mobile_basic_info"]["reg_time"] = date_compat(output_hash["mobile_basic_info"]["reg_time"])
	output_hash["mobile_basic_info"]["update_time"] = date2int(hash_link(user_data, ["c_mobile_basic", "update_time"]))
#写入es
	out_body = gen_store_doc_bodies(INDEX, TYPE, output_hash, BODY_QUEUE, 3000)
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
