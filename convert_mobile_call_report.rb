require 'rubygems'
require 'json'
require 'time'
require './common_funcs'
require './mongo_handler'
require './sqlite_treat'
require './es_handler'
require 'thread'

INDEX = "mobile_call_report"
TYPE = "credit_data"
SQLDB = MyDB.new("ids.db", "id_pairs")
ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")
body_queue0=[]; body_queue1=[]; body_queue2=[]; body_queue3=[]; body_queue4=[]; body_queue5=[]
body_queue6=[]; body_queue7=[];body_queue8=[];body_queue9=[]
threads = []
#防止线程无警告中断
Thread.abort_on_exception = true

def gen_thr(filename, body_queue)
	open(filename, 'r') do |fin|
		fin.each do |line|
			output_hash = Hash.new
			line.chomp!
			input_hash = JSON.parse(line)
			output_hash["old_id"] = input_hash["_id"]
			#忽略身份证号不存在记录
			next unless output_hash["old_id"].kind_of? String
			#忽略call记录为空记录
			next unless input_hash["l_report_contact_list"] and input_hash["l_report_contact_list"].size > 0
			output_hash["report_id"] = SQLDB.fetch_from_id(output_hash["old_id"])
			output_hash["report_detail_list"] = input_hash["l_report_contact_list"]
			output_hash["update_time"] = Time.now.to_i
	#写入es
			out_body = gen_store_doc_bodies(INDEX, TYPE, output_hash, body_queue, 100)
			ES_DB.bulk_push(out_body) if out_body.is_a? Array
		end
		if body_queue.size > 0
			out_body = gen_remain_store_bodies(INDEX, TYPE, body_queue)
			ES_DB.bulk_push(out_body)
		end
	end
end

threads << Thread.new { gen_thr("/home/work/mobile_call_report_000", body_queue0)}
threads << Thread.new { gen_thr("/home/work/mobile_call_report_001", body_queue1)}
threads << Thread.new { gen_thr("/home/work/mobile_call_report_002", body_queue2)}
threads << Thread.new { gen_thr("/home/work/mobile_call_report_003", body_queue3)}
threads << Thread.new { gen_thr("/home/work/mobile_call_report_004", body_queue4)}
threads << Thread.new { gen_thr("/home/work/mobile_call_report_005", body_queue5)}
threads << Thread.new { gen_thr("/home/work/mobile_call_report_006", body_queue6)}
threads << Thread.new { gen_thr("/home/work/mobile_call_report_007", body_queue7)}
threads << Thread.new { gen_thr("/home/work/mobile_call_report_008", body_queue8)}
threads << Thread.new { gen_thr("/home/work/mobile_call_report_009", body_queue9)}

threads.map(&:join)
