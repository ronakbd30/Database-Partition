#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import numpy as np


DATABASE_NAME = 'ronak_dds_assignment1'



def getopenconnection(user='postgres', password='ronak', dbname='ronak_dds_assignment1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    #path2 = 'C:/Users/Ronak/Desktop/ASU MCS/sem3/DDS/Assignmets/Assignment1/ml-10m/ml-10M100K/ratings_new.txt'   
          
    file = open(ratingsfilepath,'r')
    
    file2 = open(ratingsfilepath+"_new",'w')
    line = file.readline()
    
    while line:
        temp=line.split('::')                
        file2.write(temp[0]+"\t"+temp[1]+"\t"+temp[2]+"\n")
        line = file.readline()
    file.close()
    file2.close()
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    try:
        cur.execute("CREATE TABLE "+ratingstablename+"(userid INTEGER, movieid INTEGER, rating double precision);")
        file = open(ratingsfilepath+"_new",'r')
        cur.copy_from(file, ratingstablename)
    except:
        print "Table exists"  
    
    file.close()
    cur.close()
    pass


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    
    try:
        cur.execute("CREATE TABLE " + ratingstablename +"one_partition_count (count INTEGER);")
        cur.execute("INSERT INTO " + ratingstablename +"one_partition_count "+ "VALUES ("+str(numberofpartitions)+");")
    except:
        pass
    
    
    increment = float(5)/numberofpartitions
    i=0;
    j=0;
    while i<5:
        k = i + increment
        try:
            cur.execute("CREATE TABLE " + ratingstablename +"_range_"+str(j)+" (userid INTEGER, movieid INTEGER, rating double precision);")
            if i==0:
                cur.execute("INSERT INTO " + ratingstablename + "_range_"+str(j) + " SELECT userid, movieid, rating FROM "+ratingstablename+ " WHERE rating>=" + str(i) + " AND Rating <="+ str(k) +";")
            else:
                cur.execute("INSERT INTO " + ratingstablename + "_range_"+str(j) + " SELECT userid, movieid, rating FROM "+ratingstablename+ " WHERE rating>" + str(i) + " AND Rating <="+ str(k) +";")
        except:
            print "table "+ratingstablename +"_range_"+str(j)+" exists"
            
        i = i+increment
        j = j+1;     
    cur.close();
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    try:
        cur.execute("CREATE TABLE " + ratingstablename +"two_partition_count (count INTEGER);")
        cur.execute("INSERT INTO " + ratingstablename +"two_partition_count "+ "VALUES ("+str(numberofpartitions)+");")
    except:
        pass
    boole=0
    i=0
    while i<numberofpartitions:
        try:
            cur.execute("CREATE TABLE " + ratingstablename +"_rr_"+str(i)+" (userid INTEGER, movieid INTEGER, rating double precision);")
        except:
            boole=7
            print "table "+ratingstablename +"_rr_"+str(j)+" exists"
        i=i+1

    cur.execute("SELECT * FROM "+ratingstablename+";")
    lines = cur.fetchall()

    if boole==0:
        i=0
        for line in lines:
            cur.execute("INSERT INTO " + ratingstablename + "_rr_"+str(i)+ " VALUES "+str(line)+";")
            i=(i+1)%numberofpartitions
    cur.close()
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    cur.execute("SELECT * FROM "+ratingstablename)
    rows=cur.fetchall()
    i=0
    for row in rows:
        i=i+1;

    cur.execute("SELECT count FROM "+ratingstablename+"two_partition_count")
    partition=cur.fetchone()[0]
    
    index=i%partition
    cur.execute("INSERT INTO " + ratingstablename + "_rr_"+str(index) + " VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+");")
    cur.execute("INSERT INTO " + ratingstablename + " VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+");")
    print "one row inserted"
    cur.close()    
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()

    cur.execute("SELECT count FROM "+ratingstablename+"one_partition_count")
    partition=cur.fetchone()[0]
    
    increment = float(5)/partition
    #print increment
    #print rating
    i=0;
    j=0;
    while i<5:
        #print i
        k = i + increment
        if (rating>i and rating<=k):            
            cur.execute("INSERT INTO " + ratingstablename + "_range_"+str(j) + " VALUES ("+str(userid)+","+str(itemid)+","+str(rating)+");")
            break
        i = i+increment
        j = j+1;
            
        
    cur.close();
    pass


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    rat="ratings"
    i=0
    cur.execute("SELECT count FROM "+rat+"two_partition_count")
    partition=cur.fetchone()[0]
    
    while i<partition:
        cur.execute("DROP table "+rat+"_rr_"+str(i)+";")
        i=i+1


    j=0
    cur.execute("SELECT count FROM "+rat+"one_partition_count")
    partition=cur.fetchone()[0]
    
    while j<partition:
        cur.execute("DROP table "+rat+"_range_"+str(j)+";")
        j=j+1

    cur.execute("DROP table "+rat+"one_partition_count;")
    cur.execute("DROP table "+rat+"two_partition_count;")
    cur.close()


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:
        
        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)
            path = 'C:/Users/Ronak/Desktop/ASU MCS/sem3/DDS/Assignmets/Assignment1/ml-10m/ml-10M100K/rating_test.dat'
            table_name='ratings'
            
            
            # Here is where I will start calling your functions to test them. For example,



            
            #loadratings('ratings',path, con)
            #rangepartition('ratings',5,con)
            #roundrobinpartition('ratings', 5, con)
            #rangeinsert('ratings', 3, 1000011, 0, con)
            
            #roundrobininsert('ratings', 2, 1001, 3, con)
            delete_partitions('ratings',con)



            
            
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
