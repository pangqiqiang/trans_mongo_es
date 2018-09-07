#!/usr/bin/env ruby
#-*-coding:utf-8-*-
require "./es_handler"
require "./mysql_deal"

ES_DB = ELS.new("10.111.30.171:9200", "10.111.30.172:9200", "10.111.30.173:9200")
MSQLDB = Mysql_DB.new("10.111.30.20", 3306, "op", "KRkFcVCbopZbS8R7", "jjd_11th",  "user_passport")

names = ["訾渊","孙新超","贾海娜","春宵","刘春肖"]
names.each do |name|
    id, puid = ES_DB.search_by_name("user_info","credit_data",name)
    new_id = MSQLDB.get_from_salt(puid)
    ES_DB.update("user_info","credit_data", id, {puid: new_id})
end