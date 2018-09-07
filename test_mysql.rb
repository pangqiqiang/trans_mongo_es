require "./mysql_deal"

MyDB = Mysql_DB.new("10.111.30.20", 3306, "op", "KRkFcVCbopZbS8R7", "jjd_11th",  "user_passport")
puts MyDB.get_from_salt("19b80d6d-423a-425c-9589-ad57e27d8962")
