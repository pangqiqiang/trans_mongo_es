#!/usr/bin/env ruby
#-*-coding:utf-8-*-
require 'rubygems'
require 'sqlite3'

class MyDB

	def initialize(dbfile, table_name)
		@db = SQLite3::Database.new(dbfile)
		@table_name = table_name
		@db.execute <<-SQL
			CREATE TABLE IF NOT EXISTS #{table_name}
			(id TEXT PRIMARY KEY  NOT NULL, report_id TEXT, uid TEXT);
		SQL
	end

	def store(id, report_id, uid)
		@db.execute("INSERT INTO #{@table_name} VALUES (?,?,?)", [id, report_id, uid])
	end

	def fetch_from_id(id)
		@db.execute("SELECT report_id FROM #{@table_name} WHERE id=?", id)[0][0] rescue nil
	end

	def fetch_from_uid(uid)
		@db.execute("SELECT report_id FROM #{@table_name} WHERE uid=?", uid)[0][0] rescue nil
	end

	def create_index
		@db.execute "CREATE INDEX ID_INDEX ON #{@table_name} (id)"
		@db.execute "CREATE INDEX ID_INDEX ON #{@table_name} (uid)"
	end

	def bulk_store(list)
		@db.execute "BEGIN TRANSACTION"
		list.each do |item|
			@db.execute("INSERT INTO #{@table_name} VALUES (?,?,?)", item)
		end
		@db.execute "COMMIT TRANSACTION"
	end

end
