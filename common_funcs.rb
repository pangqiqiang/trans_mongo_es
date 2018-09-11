#!/usr/bin/env ruby
#-*-coding:utf-8-*-
require 'time'

def bool2int(bool, time)
	return 3 if bool
	return 4 if time
	return 0
end

def date2int(time)
	begin
		case time
		when String
			datetime = Time.parse(time)
		when Hash
			datetime = Time.parse(time["$date"])
		end
		return datetime.to_i if datetime
	rescue
		return
	end
end

def date_compat(time)
	case time
	when String
		return time
	when Hash
		return time["$date"]
	end
	return time
end

def rename_hash_item(inobj, outobj, keys)
	return if not inobj
	keys.each { |key| outobj[key] = inobj && inobj[key] }
end


def hash_link(nest, keys)
	return if not nest
	result = nest
begin
	keys.each do |key|
		result = result[key]
	end
rescue NoMethodError
	return
end
	return result
end


def transfer_list(list_in, list_out, key)
	return unless list_in.is_a? Array
	list_in.each do |item|
		list_out << item[key]
	end
end

def gen_id_body(index, type, id, output_hash)
	{_index:index, _type:type, _id:id, data: output_hash}
end

def gen_store_doc_bodies(doc, list, limit)
	list << doc
	return if list.size < limit
	body = Array.new
	while item = list.shift
		each_doc = {index: item}
		body << each_doc
	end
	return body
end

def gen_remain_store_bodies(list)
	body = Array.new
	while item = list.shift
		each_doc = {index: item}
		body << each_doc
	end
	return body
end


def gen_update_doc_bodies(index, type, doc, list, key, limit)
	list << doc
	return if list.size < limit
	body = Array.new
	while item = list.shift
		each_doc = {update: { _index: index, _type: type, _id: item[key], data: {doc: item} }}
		body << each_doc
	end
	return body
end

def gen_remain_update_bodies(index, type, list, key)
	body = Array.new
	while item = list.shift
		each_doc = {update: { _index: index, _type: type, _id: item[key], data: {doc: item} }}
		body << each_doc
	end
	return body
end


def gen_sql_list(item, list, limit)
	list << item
	return if list.size < limit
	body = Array.new
	while item = list.shift
		body << item
	end
	return body
end
