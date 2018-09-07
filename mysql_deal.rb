require "rubygems"
require "mysql2"
class Mysql_DB
    def initialize(host,port,user,pass,db,tablename)
        @client = Mysql2::Client.new host: host, username: user, password: pass, database: db, port: port
        @tablename = tablename
    end

    def get_from_salt(salt)
        return "" if salt.size == 0 
        result =  @client.query("SELECT uid FROM #{@tablename} WHERE salt ='#{salt}'")
        return result.first["uid"] if result.first
    end

    def get_face_from_uid(uid)
        return "" if uid.size == 0
        result =  @client.query("SELECT * FROM #{@tablename} WHERE id = '#{uid}'")
        return result.first["b_compare_status"], result.first["t_update_tm"].to_s if result.first
    end
end     