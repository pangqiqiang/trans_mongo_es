require "rubygems"
require "mysql2"
class Mysql_DB

    def initialize(host,port,user,pass,db,tablename)
        @client = Mysql2::Client.new host: host, username: user, password: pass, database: db, port: port
        @tablename = tablename
    end

    def get_from_salt(salt)
        return "NULL" if salt.size == 0 
        statement = @client.prepare("SELECT uid FROM #{@tablename} WHERE salt = ?")
        result = statement.execute(salt)
        return result.first["uid"] rescue NULL
    end
end     