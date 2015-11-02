#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    partition=0
    file = open("RangeQueryOut.txt",'w')
    
    cur.execute("SELECT COUNT(*) FROM range" + ratingsTableName +"metadata")
    partition=cur.fetchone()[0]
    
    
    
    i=0
    while i<partition:
        
        cur.execute("SELECT * FROM range" + ratingsTableName +"part"+str(i)+" WHERE rating>="+str(ratingMinValue)+" AND "+"rating<="+str(ratingMaxValue))
        lines = cur.fetchall()
        for line in lines:
            file.write("range"+ratingsTableName +"part"+str(i)+" , "+str(line[0])+" , "+str(line[1])+" , "+str(line[2])+"\n")
            
        i=i+1

    cur.execute("SELECT partitionnum FROM roundrobin" + ratingsTableName +"metadata")
    partition=cur.fetchone()[0]
    
    i=0;
    while i<partition:        
        cur.execute("SELECT * FROM roundrobin" + ratingsTableName +"part"+str(i)+" WHERE rating>="+str(ratingMinValue)+" AND "+"rating<="+str(ratingMaxValue))
        lines = cur.fetchall()
        for line in lines:
            file.write("roundrobin"+ratingsTableName +"part"+str(i)+" , "+str(line[0])+" , "+str(line[1])+" , "+str(line[2])+"\n")
            
        i=i+1
    file.close()
    cur.close()

def PointQuery(ratingsTableName, ratingValue, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    partition=0
    file = open("PointQueryOut.txt",'w')
    cur.execute("SELECT COUNT(*) FROM range" + ratingsTableName +"metadata")
    partition=cur.fetchone()[0]
    
    
    
    i=0
    while i<partition:
        
        cur.execute("SELECT * FROM range" + ratingsTableName +"part"+str(i)+" WHERE rating="+str(ratingValue))
        lines = cur.fetchall()
        for line in lines:
            file.write("range"+ratingsTableName +"part"+str(i)+" , "+str(line[0])+" , "+str(line[1])+" , "+str(line[2])+"\n")
            
        i=i+1

    cur.execute("SELECT partitionnum FROM roundrobin" + ratingsTableName +"metadata")
    partition=cur.fetchone()[0]
    
    i=0;
    while i<partition:
        cur.execute("SELECT * FROM roundrobin" + ratingsTableName +"part"+str(i)+" WHERE rating="+str(ratingValue))
        lines = cur.fetchall()
        for line in lines:
            file.write("roundrobin"+ratingsTableName +"part"+str(i)+" , "+str(line[0])+" , "+str(line[1])+" , "+str(line[2])+"\n")
            
        i=i+1
    file.close()
    cur.close()
