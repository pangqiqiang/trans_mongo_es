#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require "./es_handler"
require "json"

ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")

#output_hash={"old_id" => "1234567908"}
#report_id = ES_DB.store("user_info", "history", output_hash)

#output_hash["puid"] = "5699789"
#output_hash["report_id"] = report_id
#output_hash["quid"] = "213242334324"

#ES_DB.update("user_info", "history", report_id, output_hash)

#puts ES_DB.search_with_puid("user_info","credit_data","201708161330119638")
#puts ES_DB.search_by_name("user_info","credit_data","刘春肖")
puts ES_DB.search_by_query("user_info", "credit_data", {user_name:"刘春肖"})