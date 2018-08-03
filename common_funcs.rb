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
