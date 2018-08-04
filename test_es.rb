#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require "./es_handler"
require "json"

ES_DB = ELS.new("192.168.30.209:9200", "192.168.30.207:9200", "192.168.30.208:9200")

output_hash={"old_id" => "1234567908"}
report_id = ES_DB.store("test_user_info", "history", output_hash)

output_hash["puid"] = "5699789"
output_hash["report_id"] = report_id
output_hash["quid"] = "213242334324"

ES_DB.update("test_user_info", "history", report_id, output_hash)
