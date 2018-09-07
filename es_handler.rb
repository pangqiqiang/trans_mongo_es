#!/usr/bin/env ruby
#-*-coding:utf-8-*-

require 'rubygems'
require 'elasticsearch'
require 'hashie'

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

	def search_with_puid(index, type, puid)
		response = @client.search index:index, type:type, body: {query: { match: { puid: puid } }}
		response = Hashie::Mash.new response
		#response.hits.hits.first._source.puid
		return response.hits.hits.first._id, response.hits.hits.first._source.puid
	end

	def search_by_name(index, type, name)
		response = @client.search index:index, type:type, body: {query: { match: { user_name: name } }}
		response = Hashie::Mash.new response
		#response.hits.hits.first._source.puid
		return response.hits.hits.first._id, response.hits.hits.first._source.puid
	end

	def search_by_query(index, type, query)
		response = @client.search index:index, type:type, body: {query: { match: query}}
		response = Hashie::Mash.new response
		response.hits.hits.first._id
	end
end
