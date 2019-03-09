import datetime

import mysql.connector
import pymongo


def get_MySQL_Cnx():
    # For local use
    cnx = mysql.connector.connect(user='db_user', password='db_password',
                              host='127.0.0.1',
                              database='employees')
    return cnx


def get_employee_departments(_cnx, emp_no):
        _cursor = _cnx.cursor()
        departments = []
        subquery = ("SELECT dept_no, from_date, to_date "
                    "FROM employees.dept_emp WHERE emp_no = %(emp_no)s ")
        #print "subquery {} ".format(subquery)
        
        _cursor.execute(subquery, { "emp_no": emp_no })

        for (dept_no, from_date, to_date) in _cursor:
            dpt = get_department(cnx_3, dept_no)
            #print( "dpt {} ".format(dpt))
            department = {
                    "dept_no" : dpt['dept_no'],
                    "dept_name" : dpt['dept_name'],
                    "from_date" : datetime.datetime.strptime(from_date.isoformat(), "%Y-%m-%d"),
                    "to_date" : datetime.datetime.strptime(to_date.isoformat(), "%Y-%m-%d"),
                    }

            departments.append(department)
            
        _cursor.close()
        
        return departments

def get_department(_cnx, dept_no):
        _cursor = _cnx.cursor()
        subquery = ("SELECT dept_name "
                    "FROM employees.departments WHERE dept_no = %(dept_no)s ")
        # print( "subquery {} ".format(subquery))
        
        _cursor.execute(subquery, { "dept_no": dept_no })
        
        department = {}
        for (dept_name) in _cursor:
            department = {
                    "dept_no" : dept_no,
                    "dept_name" : dept_name[0]
                    }
            #print "Adding salary {}".format(salary)
            
        _cursor.close()
        
        return department



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

def get_employee_titles(_cnx, emp_no):
        _cursor = _cnx.cursor()
        titles = []

        subquery = ("SELECT title, from_date, to_date "
                    "FROM employees.titles WHERE emp_no = %(emp_no)s ")
        #print "subquery {} ".format(subquery)
        
        _cursor.execute(subquery, { "emp_no": emp_no })

        for (title, from_date, to_date) in _cursor:
            title = {
                    "title" : title,
                    "from_date" : datetime.datetime.strptime(from_date.isoformat(), "%Y-%m-%d"),
                    "to_date" : datetime.datetime.strptime(to_date.isoformat(), "%Y-%m-%d"),
                    }
            #print "Adding title {}".format(title)

            titles.append(title)
    
        _cursor.close()
        
        return titles

def migrate_employee_data():
# Connection to Mongo DB
    cursor = cnx.cursor()
    mdb_cnx = get_MDB_cnx()
    
    print("Connection established successfully!!!")
    print("{}".format(datetime.datetime.now()))
    
    query = ("SELECT emp_no, birth_date, first_name, last_name, gender, hire_date "
             "FROM employees.employees LIMIT 2000")
    
    cursor.execute(query)
    
    for (emp_no, birth_date, first_name, last_name, gender, hire_date) in cursor:
         
        employee={
            "emp_no": emp_no, 
            "first_name": first_name, 
            "last_name": last_name, 
            "gender": gender,
            "birth_date":  datetime.datetime.strptime(birth_date.isoformat(), "%Y-%m-%d"),
            "hire_date":  datetime.datetime.strptime(hire_date.isoformat(), "%Y-%m-%d"),
            "current_department": "",
            "current_title": "",
            "current_salary": "",
            "departments": [],
            "salaries": [],
            "titles": []
            }
    
        employee['titles'] = get_employee_titles(cnx_2, emp_no)
        last_item = len(employee['titles']) - 1;
        employee['current_title'] = employee['titles'][last_item]['title']

        employee['departments'] = get_employee_departments(cnx_2, emp_no)
        last_item = len(employee['departments']) - 1;
        employee['current_department'] = employee['departments'][last_item]['dept_name']

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
    collection = conn.test.employees
    emp_id = collection.insert_one(employee)
    return emp_id

def get_MDB_cnx():
    # For local use
    # conn = pymongo.MongoClient("mongodb://demo:demo00@mycluster0-shard-00-00.mongodb.net:27017,mycluster0-shard-00-01.mongodb.net:27017,mycluster0-shard-00-02.mongodb.net:27017/admin?ssl=true&replicaSet=Mycluster0-shard-0&authSource=admin")
    conn=pymongo.MongoClient("mongodb://localhost:27017")
    return conn

if __name__ == "__main__":
    # For local use
    cnx = get_MySQL_Cnx()
    cnx_2 = get_MySQL_Cnx()
    cnx_3 = get_MySQL_Cnx()
    migrate_employee_data()
    
