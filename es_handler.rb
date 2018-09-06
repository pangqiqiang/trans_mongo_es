#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'elasticsearch'

class ELS
	def initialize(*hosts)
		@client = Elasticsearch::Client.new hosts:hosts, request_timeout:5*60, randomize_hosts: true
	end

	def store(index, type, doc)
		result = @client.index index:index, type:type, body:doc
		result["_id"]
	end


	def update(index, type, id, doc)
		@client.update index:index, type:type, id:id, body: { doc: doc }
	end

	def bulk_push(docs)
		@client.bulk body: docs
	end
end
