This project requires JDK 8, MySQL, and Maven.  Please install them first. I visualize result by html and I have only tested the web page in Chrome web browser.

How to config:

1. Initialize database. Open mysql shell by

		mysql -u root -p

	In mysql shell, run

		create database quant;

	The above command means to create a database called quant. We will need this database name later.

	Get back to terminal. At src/main/sql, run

		mysql database_name_here < table-seed.sql -u username_here -p

2. Set your mysql username, password, database name, host address in
src/main/resources/config.properties. I have created a config.properties.example.
You should copy one, name it as config.properties, and put in your configuration.