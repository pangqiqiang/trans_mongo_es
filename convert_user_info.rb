#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './sqlite_treat'
file_input = "/tmp/user_info.json"
file_output = "/tmp/user_info_out.json"
SQLDB = MyDB.new("ids.db", "id_pairs")

do_each_row = Proc.new do |fin, fout, line|
	output_hash = Hash.new
	line.chomp!
	input_hash = JSON.parse(line)
	output_hash["report_id"] = fin.lineno
	output_hash["_id"] = output_hash["report_id"]
	output_hash["old_id"] = input_hash["_id"]
#忽略身份证号不存在记录
	next unless output_hash["old_id"].kind_of? String
	output_hash["puid"] = hash_link(input_hash, ["l_business_system", 0, "_id"])
#维护report_id, id, uid映射关系
	SQLDB.store(output_hash["old_id"], output_hash["report_id"], output_hash["puid"])
	output_hash["quid"] = hash_link(input_hash, ["l_business_system", 0, "_id"])
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
#写入json
	fout.puts(output_hash.to_json)
end

File.open(file_input, "r") do |fin|
	File.open(file_output, "w") do |fout|
		fin.each do |line|
			do_each_row.call(fin, fout, line)
		end
	end
end
