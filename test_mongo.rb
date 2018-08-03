#!/usr/bin/env ruby
#-*-coding:utf-8-*-
#
#
require './mongo_handler'

DB = Deal_Mongo.new("10.25.141.106:18000", "credit", "c_user_data", "trans", "123456")

puts DB.fetch_item_from_id("342427199603284413")["c_jingdong_basic"]
puts DB.fetch_item_from_id("342427199603284413")["c_mobile_basic"]
