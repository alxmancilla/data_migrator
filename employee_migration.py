import datetime

import mysql.connector
import pymongo


def get_MySQL_Cnx():
    # For local use
    cnx = mysql.connector.connect(user='demo', password='demo00',
                              host='127.0.0.1',
                              database='employees')
    return cnx

def get_MDB_cnx():
    # For local use
    # conn = pymongo.MongoClient("mongodb://demo:demo00@mycluster0-shard-00-00.mongodb.net:27017,mycluster0-shard-00-01.mongodb.net:27017,mycluster0-shard-00-02.mongodb.net:27017/admin?ssl=true&replicaSet=Mycluster0-shard-0&authSource=admin")
    conn=pymongo.MongoClient("mongodb://localhost:27017")
    return conn


def get_employee_salaries(_cnx, emp_no):
        _cursor = _cnx.cursor()
        salaries = []
        subquery = ("SELECT salary, from_date, to_date "
                    "FROM employees.salaries WHERE emp_no = %(emp_no)s ")
        #print "subquery {} ".format(subquery)
        
        _cursor.execute(subquery, { "emp_no": emp_no })

        for (salary, from_date, to_date) in _cursor:
            salary = {
                    "salary" : salary,
                    "from_date" : datetime.datetime.strptime(from_date.isoformat(), "%Y-%m-%d"),
                    "to_date" : datetime.datetime.strptime(to_date.isoformat(), "%Y-%m-%d"),
                    }
            #print "Adding salary {}".format(salary)

            salaries.append(salary)
            
        _cursor.close()
        
        return salaries


def migrate_employee_data():
# Connection to Mongo DB
    cursor = cnx.cursor()
    mdb_cnx = get_MDB_cnx()
    
    print("Connection established successfully!!!")
    print("{}".format(datetime.datetime.now()))
    
    query = ("SELECT emp_no, birth_date, first_name, last_name, gender, hire_date "
             "FROM employees.employees LIMIT 1000")
    
    cursor.execute(query)
    
    for (emp_no, birth_date, first_name, last_name, gender, hire_date) in cursor:
         
        employee={
            "emp_no": emp_no, 
            "first_name": first_name, 
            "last_name": last_name, 
            "gender": gender,
            "birth_date":  datetime.datetime.strptime(birth_date.isoformat(), "%Y-%m-%d"),
            "hire_date":  datetime.datetime.strptime(hire_date.isoformat(), "%Y-%m-%d"),
            "current_salary": "",
            "salaries": [],
            }
    
        employee['salaries'] = get_employee_salaries(cnx_2, emp_no)
        last_item = len(employee['salaries']) - 1;
        employee['current_salary'] = employee['salaries'][last_item]['salary']


        
        #print "Inserting employee {}".format(emp_no)
        # inserting the data into MongoDB  database
        #print(".", end=' ')
        insert_employee_data(mdb_cnx, employee)
        
    print(".")
    print("{}".format(datetime.datetime.now()))
    print("Migration completed successfully!!!")
      
    cursor.close()
    cnx.close()
    cnx_2.close()
    cnx_3.close()
    mdb_cnx.close()


def insert_employee_data(conn, employee):
    collection = conn.demo.employees
    emp_id = collection.insert_one(employee)
    return emp_id



if __name__ == "__main__":
    # For local use
    cnx = get_MySQL_Cnx()
    cnx_2 = get_MySQL_Cnx()
    cnx_3 = get_MySQL_Cnx()
    start_time = datetime.datetime.utcnow()
    migrate_employee_data()
    end_time = datetime.datetime.utcnow()
    print("end time: ", end_time)
    print( (end_time - start_time), " seconds")
    
