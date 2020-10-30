import unittest
import cassandra

import connection
import init_db
import config

import datetime

from resolve import *
import traverse

cluster = cassandra.cluster.Cluster()
session = cluster.connect()
session.execute("CREATE KEYSPACE testdb_scenario WITH REPLICATION = {};".format({'class' : 'SimpleStrategy',
	'replication_factor' : 1}))
session.set_keyspace('testdb_scenario')        
init_db.create_table_identities(session)
init_db.create_table_events(session)

dict_to_insert = {"mob":"m1","event":"E1"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()


dict_to_insert = {"fb":"fb1","event":"E2"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"mob":"m1","fb":"fb1","event":"E3"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"email":"em1","event":"E4"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"cust":"C1","email":"em1","fb":"fb3","event":"E5"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"mob":"m2","fb":"fb1","event":"E6"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"email":"em2","event":"E7"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"cust":"C2","email":"em1","fb":"fb4","event":"E8"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"cust":"C3","fb":"fb4","event":"E9"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"fb":"fb4","event":"E10"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"cust":"C4","email":"em5","event":"E11"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

dict_to_insert = {"email":["em1","em2"],"fb":["fb1","fb4"],"event":"E12"}
print("Inserting dict : {}".format(dict_to_insert))
r = input()
main(session,dict_to_insert)
print("------Identities Table-------")
result = session.execute("SELECT * FROM identities")
for row in result:
	print(row)
	r = input()
print("------Events Table-------")
result = session.execute("SELECT * FROM events")
for row in result:
	print(row)
	r = input()

while(1):
	cdp_uid_to_traverse = int(input("Enter cdp_uid to traverse:"))
	traverse.main(session,cdp_uid_to_traverse)