from config import *
from connection import *
from identities import *

session = get_connection({},CASSANDRA_KEYSPACE)

test_identity = "roy@gmail.com"
test_identity_type = "mail"
test_cid = 123
test_uid = 1111
test_uid_update = 89

input()
put(session,test_identity,test_identity_type,test_cid,test_uid)

input()
uid_fetched = get(session,test_identity,test_identity_type,test_cid)
print("Fetched uid : {}".format(uid_fetched))

input()
update(session,test_identity,test_identity_type,test_cid,test_uid_update)
uid_fetched = get(session,test_identity,test_identity_type,test_cid)
print("Fetched uid : {}".format(uid_fetched))

input()
delete(session,test_identity,test_identity_type,test_cid)
print("Row fetched after deletion : {}".format(session.execute("SELECT * from identities").one()))
