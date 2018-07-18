#!/usr/bin/python

import sys
import sqlite3
import random


class ComputingCenters:

    def __init__(self, filename):
        self.filename = filename
        self.conn = sqlite3.connect(self.filename)
        self.conn.execute('PRAGMA foreign_keys = ON')
        print("Opened database successfully")

    def create_tables(self):
        self.conn.execute('''CREATE TABLE COMPUTING_CENTERS
                 (`ID` INT PRIMARY KEY    NOT NULL,
                 `NUM_OF_NODES`  INT     NOT NULL,
                 `RAM`            INT,
                 `CPU`            INT );''')

        self.conn.execute('''CREATE TABLE NODES
                 (`ID` INT PRIMARY KEY    NOT NULL,
                 `CC_ID` INT ,
                 `RAM`            INT,
                 `CPU`            INT,
                 `CPU_TYPE`       INT,
                 FOREIGN KEY(CC_ID)REFERENCES COMPUTING_CENTERS(ID) );''')

        print("Tables created successfully")

        #self.conn.close()

    def insert_db(self):
        #self.conn = sqlite3.connect(self.filename)
        #print("Opened database successfully")

        num_cc = random.randint(5, 50)
        #print(num_cc)
        count = 1
        for i in range(1,num_cc):
            num_nodes = random.randint(1, 20)
            ram = random.randint(8, 512)
            cpu = random.randint(10, 200)

            self.conn.execute("INSERT INTO COMPUTING_CENTERS (ID,NUM_OF_NODES,RAM,CPU) \
                VALUES ("+str(i) + ","+str(num_nodes) + "," + str(ram*num_nodes) + "," + str(cpu*num_nodes)+" )");

            for j in range(0,num_nodes):
                self.conn.execute("INSERT INTO NODES (ID,CC_ID,RAM,CPU,CPU_TYPE) \
                VALUES (" + str(count) + "," + str(i) + "," + str(ram) + "," + str(cpu) + "," + str(random.choice([1,2]))+" )");

                count += 1
        self.conn.commit()
        print("insert data successfully")
        self.conn.close()