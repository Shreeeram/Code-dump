#!/usr/bin/python

import os
import sys
import getopt
import time
import informixdb

def usage(server):
print "%s -s <server> -d <database> -w <where clause> -n <num rows> -s <sleep seconds> [-v]" % sys.argv[0]
print "   -s : DBSERVERNAME, default %s" % server
print "   -d : database name, required"
print "   -w : where clause, required"
print "   -n : number of rows per transaction, optional, default 10"
print "   -s : sleep seconds in between each transaction, optional, default 1"
print "   -v : verbose output, optional, default off"

# wt4logbf executes onstat to identify the number of threads waiting for a log buffer
# if more than maxlogbfwt threads waiting on logbf are found we will sleep for sleepSeconds
#
# threads waiting for logbf is an indication that HDR is behind and if we do not throttle
# back out deletes engine performance will drop
def wt4logbf(maxlogbfwt, sleepSeconds):
# execute onstat -g ath and count the number of threads waiting on logbf
logbfwt = int(os.popen("onstat -g ath | grep logbf | wc -l").readlines()[0])

# sleep sleepSeconds and recheck number of waiting threads
# repeat until number of threads waiting for logbf is below maxlogbfwt
while logbfwt >= maxlogbfwt:
print "max logbf waits reached [%d >= %d] sleeping %d seconds" % (logbfwt, maxlogbfwt, sleepSeconds)
sys.stdout.flush()

time.sleep(sleepSeconds)

logbfwt = int(os.popen("onstat -g ath | grep logbf | wc -l").readlines()[0])

def main():
server = os.getenv("INFORMIXSERVER")
database = None
where = None
numDelPerTransaction = 10
sleepSeconds = 1
verbose = False

# parse command line arguments
try:
opts, args = getopt.getopt(sys.argv[1:], "S:d:w:n:s:v?")
except:
usage(server)
sys.exit(2)

for opt, val in opts:
if opt == "-S":
server = val
if opt == "-d":
database = val
if opt == "-w":
where = val
if opt == "-n":
numDelPerTransaction = int(val)
if opt == "-s":
sleepSeconds = int(val)
if opt == "-v":
verbose = True
if opt == "-?":
usage(server)
sys.exit()

# if the required arguments were not passed display the usage and exit
if (numDelPerTransaction < 1) or (sleepSeconds < 0) or (where is None):
usage(server)
sys.exit()

# sql to select the primary key fields (pkcol1 and pkcol2) from table1 that
# meet the user defined where clause
sqlSelect = """
select
pkcol1,
pkcol2
from
table1
where
%s
""" % (where, )

# sql to delete a row by the primary key of table1
sqlDelete = """
delete from
table1
where
pkcol1 = :1 and
pkcol2 = :2
"""

# connect to the database
try:
dbCon = informixdb.connect("%s@%s" % (database, server), autocommit = False)
except informixdb.DatabaseError, e:
print "unable to connect to %s@%s, %ld"  % (database, server, e.sqlcode)
sys.exit(2)

# define select and delete cursors
try:
dbSelectCursor = dbCon.cursor(rowformat = informixdb.ROW_AS_OBJECT, hold=True)
dbDeleteCursor = dbCon.cursor()
except informixdb.DatabaseError, e:
print "unable to define cursors, %ld"  % (e.sqlcode, )
sys.exit(2)

# set some session attributes
try:
dbSelectCursor.execute("set lock mode to wait")
dbSelectCursor.execute("set isolation dirty read")
except informixdb.DatabaseError, e:
print "unable to set session attributes, %ld"  % (e.sqlcode, )
sys.exit(2)

try:
# select the primary key of all rows in table1 that meet our where clause
dbSelectCursor.execute(sqlSelect)

numRowsInTransaction = 0
totalRows = 0
startTime = time.time()

# for each row that meets our where clause, delete it
# committing the transaction and checking engine load at the user
# defined intervals
for dbRow in dbSelectCursor:
if verbose:
print "deleting row pkcol1 = %ld and pkcol2 = %ld" % (dbRow.pkcol1, dbRow.pkcol2)

# attempt to delete this row
try:
dbDeleteCursor.execute(sqlDelete, (dbRow.pkcol1, dbRow.pkcol2))
numRowsInTransaction = numRowsInTransaction + 1
totalRows = totalRows + 1
except informixdb.DatabaseError, e:
print "unable to delete row pkcol1 = %ld and pkcol2 = %ld, %ld" % (dbRow.pkcol1, dbRow.pkcol2, e.sqlcode)

# if we have met out rows to delete per transaction limit,
# commit the transaction, sleep and check engine load
if numRowsInTransaction == numDelPerTransaction:
dbCon.commit()

print "deleted %d rows [%f rows/second], sleeping %d seconds" % (totalRows, totalRows / (time.time() - startTime), sleepSeconds)
sys.stdout.flush()

numRowsInTransaction = 0

time.sleep(sleepSeconds)
wt4logbf(2, 30)

# commit the last transaction
dbCon.commit()

print "deleted %d rows" % totalRows

except informixdb.DatabaseError, e:
print "unable to execute %s,  %ld" % (sqlSelect, e.sqlcode)
sys.exit(2)

if __name__ == "__main__":
main()