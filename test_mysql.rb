require "./mysql_deal"

#MyDB = Mysql_DB.new("10.111.30.20", 3306, "op", "KRkFcVCbopZbS8R7", "jjd_11th",  "user_passport")
FACEDB = Mysql_DB.new("10.111.20.2", 3306, "dev", "KRkFcVCbopZbS8R7", "jjd",  "t_face_verify")
#puts MyDB.get_from_salt("19b80d6d-423a-425c-9589-ad57e27d8962")
puts FACEDB.get_face_from_uid("0003fe4c-6e4f-41e0-ba42-d7b798d83ed9")
