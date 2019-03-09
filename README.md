# data_migrator
Demo script for migrating MySQL data into MongoDB. 

It also exemplifies how to translate relational data model into document data model.


## Prerrequisites

Download and install MySQL Community Server 5.7.x  and MySQL Workbench 6.3.x

https://dev.mysql.com/downloads/mysql/

https://dev.mysql.com/downloads/workbench/

Once install login as root and change temporary password using:

SET PASSWORD = PASSWORD('new_password');

Use sample employees database:

Download and install employees.sql

https://github.com/datacharmer/test_db

Create employee database and imporr sample data:
$ /usr/local/mysql/bin/mysql -u root -p --connect-expired-password  < employees.sql

Validate employee database was successfully created.
$ /usr/local/mysql/bin/mysql -u root -p --connect-expired-password  -t < test_employees_md5.sql

Validate mongodb is up and running

## Migrating from MySQL to MongoDB

$ python3 employee_migration.py
