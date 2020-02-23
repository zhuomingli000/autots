-- CREATE USER 'dbuser'@'localhost' IDENTIFIED BY 'password';
GRANT ALL ON quant.* TO 'dbuser'@'localhost' IDENTIFIED BY 'password here';
-- FLUSH PRIVILEGES;
CREATE DATABASE quant;
TRUNCATE TABLE table_name;