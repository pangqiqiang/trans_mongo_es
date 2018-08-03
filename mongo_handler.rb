#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'mongo'

class Deal_Mongo

	def initialize(host, database, collect, user, pass)
		client = Mongo::Client.new([host], :database=>database,
			:user=>user, :password=>pass, :server_selection_timeout=>3)
		@collection = client[collect]
	end

	def fetch_item_from_id(id)
		@collection.find({_id: id}).first
	end
end


