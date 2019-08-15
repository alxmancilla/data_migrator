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
    conn = pymongo.MongoClient("mongodb://localhost:27017")
    return conn

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
    
    query = ("SELECT emp_no "
             "FROM employees.employees LIMIT 500")
    
    cursor.execute(query)
    
    for item in cursor:
        emp_no = item[0]

        employee={
            "filter": {"emp_no": emp_no}, 
            "values": {"current_department": "",
                       "current_title": "",
                       "departments": [],
                       "titles": []
                    }
            }
    
        employee['values']['titles'] = get_employee_titles(cnx_2, emp_no)
        last_item = len(employee['values']['titles']) - 1;
        employee['values']['current_title'] = employee['values']['titles'][last_item]['title']

        employee['values']['departments'] = get_employee_departments(cnx_2, emp_no)
        last_item = len(employee['values']['departments']) - 1;
        employee['values']['current_department'] = employee['values']['departments'][last_item]['dept_name']

       
        #print "Inserting employee {}".format(emp_no)
        # inserting the data into MongoDB  database
        #print(".", end=' ')
        update_employee_data(mdb_cnx, employee)
        
    print(".")
    print("{}".format(datetime.datetime.now()))
    print("Migration completed successfully!!!")
      
    cursor.close()
    cnx.close()
    cnx_2.close()
    cnx_3.close()
    mdb_cnx.close()


def update_employee_data(conn, employee):
    collection = conn.demo.employees
#     print("employee filter: ", employee['filter'])
#     print("employee values: ", employee['values'])
    emp_id = collection.update_one(employee['filter'], {'$set': employee['values'] }, upsert=True)
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
    
    
