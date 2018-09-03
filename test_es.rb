#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require "./es_handler"
require "json"

ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")

output_hash={"old_id" => "1234567908"}
report_id = ES_DB.store("user_info", "history", output_hash)

output_hash["puid"] = "5699789"
output_hash["report_id"] = report_id
output_hash["quid"] = "213242334324"

ES_DB.update("user_info", "history", report_id, output_hash)
