'''
Created on Oct 12, 2015

@author: elmac
'''

from xbee import ZigBee
import serial
import MySQLdb

PORT = 'COM4'
BAUD_RATE = 9600

# Open serial port
ser = serial.Serial(PORT, BAUD_RATE)

zb = ZigBee(ser, escaped = True)

#DATA BASE CONNECTION

db = MySQLdb.connect(host="localhost",user="root",passwd="",db="b3")

# prepare a cursor object using cursor() method
cursor = db.cursor()

# execute SQL query using execute() method.
cursor.execute("SELECT VERSION()")

# Fetch a single row using fetchone() method.
data = cursor.fetchone()

print "Database version : %s " % data


def checkSistem(sistema,sistemCode):
    sql = "SELECT count(*) FROM sistema WHERE Codigo = '"+sistemCode+"'"    
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchone()        
        if results[0] == 0 :            
            sql1 = "INSERT INTO `sistema`(Codigo,Nombre) VALUES('"+sistemCode[0:2]+"','"+sistema+"')"
            try:
                cursor.execute(sql1)
                db.commit()
            except:
                "Error: unable to insert data"
                # Rollback in case there is any error
                db.rollback()               
    except:
        print "Error: unable to fecth data"    
        
def checkSensor(data):
    for i in range (0,len(data)):
        dataAux = data[i].split(',')
        sistemCode = dataAux[0]    
        sql = "SELECT count(*) FROM `sensor` WHERE Sistema = '"+sistemCode[0:2]+"' AND nombre = '"+sistemCode[2:len(sistemCode)]+"'"
           
        try:
            #Execute the SQL command
            cursor.execute(sql)
            #Fetch all the rows in a list of lists.
            results = cursor.fetchone()        
            if results[0] == 0 :                           
                sql1 = "INSERT INTO `sensor`(Sistema,nombre,alarma) VALUES('"+sistemCode[0:2]+"','"+sistemCode[2:len(sistemCode)]+"','1000')"
                try:
                    cursor.execute(sql1)
                    db.commit()
                except:
                    "Error: unable to insert data"
                    # Rollback in case there is any error
                    db.rollback()               
        except:
            print "Error: unable to fecth data"  
            
def uploadData(data):
    for i in range (0,len(data)):
        dataAux = data[i].split(',')
        sistemCode = dataAux[0]
        value = dataAux[1]
        
        sql = "SELECT * FROM `sensor` WHERE Sistema = '"+sistemCode[0:2]+"' AND nombre = '"+sistemCode[2:len(sistemCode)]+"'"
                
        try:
            # Execute the SQL command
            cursor.execute(sql)
            # Fetch all the rows in a list of lists.
            results = cursor.fetchone()
            idSensor = results[0]
            
        except:
            print "Error: unable to fecth data"  
            
                        
        sql1 = "INSERT INTO `temperatura`(idsensor,temperatura) VALUES('"+str(idSensor)+"','"+value+"')"
        try:
            cursor.execute(sql1)
            db.commit()
        except:
            "Error: unable to insert data"
            # Rollback in case there is any error
            db.rollback()               
        
                        

while True:
    try:
        
        dataXBee = zb.wait_read_frame()['rf_data'].decode("utf-8").split(":") 
        sistema = dataXBee[0]
        data = dataXBee[1].split(";") 
        
        checkSistem(sistema,data[0].split(',')[0])
        checkSensor(data)
        uploadData(data) 
        
    except KeyboardInterrupt:
        break