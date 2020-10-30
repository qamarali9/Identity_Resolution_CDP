import unittest
import cassandra

import connection
import init_db
#import identities
import config

import datetime

from resolve import *



class TestIdentityResolution(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.cluster = cassandra.cluster.Cluster()
        self.session = self.cluster.connect()
        self.session.execute("CREATE KEYSPACE testdb WITH REPLICATION = {};".format({'class' : 'SimpleStrategy',
            'replication_factor' : 1
            }))
        self.session.set_keyspace('testdb')        
        init_db.create_table_identities(self.session)

    # @classmethod
    # def tearDownClass(self):
    #     self.session.execute("DROP KEYSPACE testdb;")
    
    def tearDown(self):
        self.session.execute("TRUNCATE identities;")

    def test_identityResolution_connection(self):
        session = connection.get_connection({},'testdb')
        self.assertEqual(type(session), cassandra.cluster.Session, "Should be cassandra.cluster.Session")

    # def test_identityResolution_put(self):
    #     init_db.create_table(self.session)
    #     identities.put(self.session,"mark@gmail.com","mail",123,76)
    #     row = self.session.execute("SELECT * FROM identities;").one()
    #     self.assertEqual(row.identity,"mark@gmail.com","Identity should be 'mark@gmail.com'")
    #     self.assertEqual(row.type,"mail","Type should be 'mail'")
    #     self.assertEqual(row.cid, 123,"cid should be 123")
    #     self.assertEqual(row.uid, 76,"uid should be 76")
    #     self.session.execute("DROP TABLE identities;")

    # def test_identityResolution_get(self):
    #     init_db.create_table(self.session)
    #     identities.put(self.session,"mark@gmail.com","mail",123,76)
    #     row = identities.get(self.session,"mark@gmail.com","mail",123)
    #     self.assertEqual(row.uid, 76,"uid should be 76")
    #     self.session.execute("DROP TABLE identities;")

    # def test_identityResolution_update(self):
    #     init_db.create_table(self.session)
    #     identities.put(self.session,"mark@gmail.com","number",123,98)
    #     identities.update(self.session,"mark@gmail.com","mail",123,76)
    #     row = self.session.execute("SELECT * FROM identities;").one()
    #     self.assertEqual(row.identity,"mark@gmail.com","Identity should be 'mark@gmail.com'")
    #     self.assertEqual(row.type,"mail","Type should be 'mail'")
    #     self.assertEqual(row.cid, 123,"cid should be 123")
    #     self.assertEqual(row.uid, 76,"uid should be 76")
    #     self.session.execute("DROP TABLE identities;")

    # def test_identityResolution_delete(self):
    #     init_db.create_table(self.session)
    #     identities.put(self.session,"mark@gmail.com","mail",123,76)
    #     identities.delete(self.session,"mark@gmail.com","mail",123)
    #     row = self.session.execute("SELECT * FROM identities;").one()
    #     self.assertEqual(row,None,"row should be None")
    #     self.session.execute("DROP TABLE identities;")

    def test_insert_new_identity(self):
        #init_db.create_table_identities(self.session)
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "C1"
        test_result_dict["most_strict_resolver_type"] = "cust"
        insert_new_identity(self.session,test_result_dict)
        row = self.session.execute("SELECT * FROM identities;").one()
        self.assertEqual(row.identifier,"C1","identifier should be 'C1'")
        self.assertEqual(row.identifier_type,"cust","identifier_type should be 'cust'")
        self.assertEqual(row.customer_id,CUSTOMER_ID,"customer_id should be {}".format(CUSTOMER_ID))
        self.assertIsInstance(row.cdp_uid, int, "cdp_uid should be of type int")
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertEqual(row.conflicting_ids,None,"conflicting_ids should be {}".format(None))
        # print(row.created_time)
        # print(type(row.created_time))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.nature,"D","nature should be 'D'")
        self.assertEqual(row.resolver_id,"C1","resolver_id should be 'C1'")
        self.assertEqual(row.resolver_id_type,"cust","resolver_id_type should be 'cust'")
        #self.session.execute("DROP TABLE identities;")

    def test_check_exists_and_return_row(self):
        #init_db.create_table_identities(self.session)
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "C1"
        test_result_dict["most_strict_resolver_type"] = "cust"
        insert_new_identity(self.session,test_result_dict)
        row = check_exists_and_return_row(self.session,test_result_dict["most_strict_resolver"],test_result_dict["most_strict_resolver_type"])
        self.assertEqual(row.identifier,"C1","identifier should be 'C1'")
        self.assertEqual(row.identifier_type,"cust","identifier_type should be 'cust'")
        self.assertEqual(row.customer_id,CUSTOMER_ID,"customer_id should be {}".format(CUSTOMER_ID))
        self.assertIsInstance(row.cdp_uid, int, "cdp_uid should be of type int")
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertEqual(row.conflicting_ids,None,"conflicting_ids should be {}".format(None))
        # print(row.created_time)
        # print(type(row.created_time))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.nature,"D","nature should be 'D'")
        self.assertEqual(row.resolver_id,"C1","resolver_id should be 'C1'")
        self.assertEqual(row.resolver_id_type,"cust","resolver_id_type should be 'cust'")
        #self.session.execute("DROP TABLE identities;")

    def test_check_exists_and_return_row_none(self):
        #init_db.create_table_identities(self.session)
        row = check_exists_and_return_row(self.session,"dummy_id","dummy_type")
        self.assertEqual(row,None,"row should be {}".format(None))
        #self.session.execute("DROP TABLE identities;")

    def test_update_identity_deterministic(self):
        #init_db.create_table_identities(self.session)
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "C1"
        test_result_dict["most_strict_resolver_type"] = "cust"
        insert_new_identity(self.session,test_result_dict)
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "em1"
        test_result_dict["most_strict_resolver_type"] = "email"
        insert_new_identity(self.session,test_result_dict)
        row = check_exists_and_return_row(self.session,"em1","email")
        update_identity_deterministic(self.session,"em1","email",row.cdp_uid,"C1","cust")
        row = check_exists_and_return_row(self.session,"em1","email")
        self.assertEqual(row.identifier,"em1","identifier should be 'em1'")
        self.assertEqual(row.identifier_type,"email","identifier_type should be 'email'")
        self.assertEqual(row.customer_id,CUSTOMER_ID,"customer_id should be {}".format(CUSTOMER_ID))
        self.assertIsInstance(row.cdp_uid, int, "cdp_uid should be of type int")
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertEqual(row.conflicting_ids,None,"conflicting_ids should be {}".format(None))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.nature,"D","nature should be 'D'")
        self.assertEqual(row.resolver_id,"C1","resolver_id should be 'C1'")
        self.assertEqual(row.resolver_id_type,"cust","resolver_id_type should be 'cust'")
        #self.session.execute("DROP TABLE identities;")

    def test_update_identity_conflicting(self):
        #init_db.create_table_identities(self.session)
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "C1"
        test_result_dict["most_strict_resolver_type"] = "cust"
        insert_new_identity(self.session,test_result_dict)
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "C2"
        test_result_dict["most_strict_resolver_type"] = "cust"
        insert_new_identity(self.session,test_result_dict)
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "em1"
        test_result_dict["most_strict_resolver_type"] = "email"
        insert_new_identity(self.session,test_result_dict)
        row = check_exists_and_return_row(self.session,"em1","email")
        conflicting_ids_set = set(["C1","C2"])
        update_identity_conflicting(self.session,"em1","email",row.cdp_uid,conflicting_ids_set,"cust")
        row = check_exists_and_return_row(self.session,"em1","email")
        self.assertEqual(row.identifier,"em1","identifier should be 'em1'")
        self.assertEqual(row.identifier_type,"email","identifier_type should be 'email'")
        self.assertEqual(row.customer_id,CUSTOMER_ID,"customer_id should be {}".format(CUSTOMER_ID))
        self.assertIsInstance(row.cdp_uid, int, "cdp_uid should be of type int")
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertEqual(row.conflicting_ids,conflicting_ids_set,"conflicting_ids should be {}".format(conflicting_ids_set))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.nature,"C","nature should be 'C'")
        self.assertEqual(row.resolver_id,"em1","resolver_id should be 'em1'")
        self.assertEqual(row.resolver_id_type,"cust","resolver_id_type should be 'cust'")
        #self.session.execute("DROP TABLE identities;")

    def test_insert_strong_path(self):
        #init_db.create_table_identities(self.session)
        uid1 = 123
        uid2 = 456
        insert_strong_path(self.session,uid1,uid2)
        row = self.session.execute("SELECT * FROM identities;").one()
        self.assertEqual(row.identifier,str(uid1),"identifier should be {}".format(str(uid1)))
        self.assertEqual(row.identifier_type,"path","identifier_type should be 'path'")
        self.assertEqual(row.customer_id,CUSTOMER_ID,"customer_id should be {}".format(CUSTOMER_ID))
        self.assertEqual(row.cdp_uid, uid2, "cdp_uid should be {}".format(uid2))
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertEqual(row.conflicting_ids,None,"conflicting_ids should be {}".format(None))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.nature,"S","nature should be 'S'")
        self.assertEqual(row.resolver_id, None,"resolver_id should be {}".format(None))
        self.assertEqual(row.resolver_id_type,None,"resolver_id_type should be {}".format(None))
        #self.session.execute("DROP TABLE identities;")

    def test_insert_weak_path(self):
        #init_db.create_table_identities(self.session)
        uid1 = 123
        uid2 = 456
        insert_weak_path(self.session,uid1,uid2)
        row = self.session.execute("SELECT * FROM identities;").one()
        self.assertEqual(row.identifier,str(uid1),"identifier should be {}".format(str(uid1)))
        self.assertEqual(row.identifier_type,"path","identifier_type should be 'path'")
        self.assertEqual(row.customer_id,CUSTOMER_ID,"customer_id should be {}".format(CUSTOMER_ID))
        self.assertEqual(row.cdp_uid, uid2, "cdp_uid should be {}".format(uid2))
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertEqual(row.conflicting_ids,None,"conflicting_ids should be {}".format(None))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.nature,"W","nature should be 'S'")
        self.assertEqual(row.resolver_id, None,"resolver_id should be {}".format(None))
        self.assertEqual(row.resolver_id_type,None,"resolver_id_type should be {}".format(None))
        #self.session.execute("DROP TABLE identities;")

    def test_update_weak_to_strong_path(self):
        #init_db.create_table_identities(self.session)
        uid1 = 123
        uid2 = 456
        insert_weak_path(self.session,uid1,uid2)
        update_weak_to_strong_path(self.session,uid1,uid2)
        row = check_exists_and_return_row(self.session,str(uid1),"path")
        self.assertEqual(row.identifier,str(uid1),"identifier should be {}".format(str(uid1)))
        self.assertEqual(row.identifier_type,"path","identifier_type should be 'path'")
        self.assertEqual(row.customer_id,CUSTOMER_ID,"customer_id should be {}".format(CUSTOMER_ID))
        self.assertEqual(row.cdp_uid, uid2, "cdp_uid should be {}".format(uid2))
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertEqual(row.conflicting_ids,None,"conflicting_ids should be {}".format(None))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.nature,"S","nature should be 'S'")
        self.assertEqual(row.resolver_id,None,"resolver_id should be {}".format(None))
        self.assertEqual(row.resolver_id_type,None,"resolver_id_type should be {}".format(None))
        #self.session.execute("DROP TABLE identities;")

    def test_insert_identifier_with_uid_deterministic(self):
        #init_db.create_table_identities(self.session)
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "C1"
        test_result_dict["most_strict_resolver_type"] = "cust"
        insert_new_identity(self.session,test_result_dict)
        resolver_row = self.session.execute("SELECT * FROM identities;").one()
        identifier = "em1"
        identifier_type = "email"
        insert_identifier_with_uid_deterministic(self.session,identifier,identifier_type,resolver_row.cdp_uid,test_result_dict["most_strict_resolver"],test_result_dict["most_strict_resolver_type"])
        row = check_exists_and_return_row(self.session,identifier,identifier_type)
        self.assertEqual(row.identifier,"em1","identifier should be 'em1'")
        self.assertEqual(row.identifier_type,"email","identifier_type should be 'email'")
        self.assertEqual(row.customer_id,CUSTOMER_ID,"customer_id should be {}".format(CUSTOMER_ID))
        self.assertEqual(row.cdp_uid, resolver_row.cdp_uid, "cdp_uid should be of type {}".format(resolver_row.cdp_uid))
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertEqual(row.conflicting_ids,None,"conflicting_ids should be {}".format(None))
        # print(row.created_time)
        # print(type(row.created_time))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.nature,"D","nature should be 'D'")
        self.assertEqual(row.resolver_id,"C1","resolver_id should be 'C1'")
        self.assertEqual(row.resolver_id_type,"cust","resolver_id_type should be 'cust'")
        #self.session.execute("DROP TABLE identities;")

    def test_insert_identifier_with_uid_conflicting(self):        
        #init_db.create_table_identities(self.session)
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "C1"
        test_result_dict["most_strict_resolver_type"] = "cust"
        insert_new_identity(self.session,test_result_dict)
        event_resolver_row = check_exists_and_return_row(self.session,"C1","cust")
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "C2"
        test_result_dict["most_strict_resolver_type"] = "cust"
        insert_new_identity(self.session,test_result_dict)
        identifier = "em1"
        identifier_type = "email"
        conflicting_ids_set = set(["C1","C2"])
        conflicting_resolver_id_type = "cust"
        insert_identifier_with_uid_conflicting(self.session,identifier,identifier_type,event_resolver_row.cdp_uid,conflicting_ids_set,conflicting_resolver_id_type)
        row = check_exists_and_return_row(self.session,identifier,identifier_type)
        self.assertEqual(row.identifier,"em1","identifier should be 'em1'")
        self.assertEqual(row.identifier_type,"email","identifier_type should be 'email'")
        self.assertEqual(row.customer_id,CUSTOMER_ID,"customer_id should be {}".format(CUSTOMER_ID))
        self.assertEqual(row.cdp_uid, event_resolver_row.cdp_uid, "cdp_uid should be of type {}".format(event_resolver_row.cdp_uid))
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertEqual(row.conflicting_ids,conflicting_ids_set,"conflicting_ids should be {}".format(conflicting_ids_set))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.nature,"C","nature should be 'C'")
        self.assertIn(row.resolver_id,conflicting_ids_set,"resolver_id should be in {}".format(conflicting_ids_set))
        self.assertEqual(row.resolver_id_type,"cust","resolver_id_type should be 'cust'")
        #self.session.execute("DROP TABLE identities;")

    def test_insert_new_event(self):
        init_db.create_table_events(self.session)
        event = "E123"
        test_result_dict = {} 
        test_result_dict["most_strict_resolver"] = "C1"
        test_result_dict["most_strict_resolver_type"] = "cust"
        insert_new_identity(self.session,test_result_dict)
        event_resolver_row = self.session.execute("SELECT * FROM identities;").one()
        insert_new_event(self.session,event,event_resolver_row)
        row = self.session.execute("SELECT * FROM events;").one()
        self.assertEqual(row.event,"E123","event should be 'E123'")
        self.assertEqual(row.cdp_uid,event_resolver_row.cdp_uid,"cdp_uid should be {}".format(row.cdp_uid))
        self.assertEqual(row.active,True,"active should be {}".format(True))
        self.assertIsInstance(row.created_time, datetime.datetime, "created_time should be of type datetime.datetime")
        self.assertEqual(row.resolver_id,"C1","resolver_id should be 'C1'")
        #self.session.execute("DROP TABLE events;")
 
if __name__ == '__main__':
    unittest.main()


"""

'''
class TestDB(unittest.TestCase):
    scenarios = [
        ('mysql', dict(database_connection=os.getenv("MYSQL_TEST_URL")),
        ('postgresql', dict(database_connection=os.getenv("PGSQL_TEST_URL")),
    ]

    def setUp(self):
       if not self.database_connection:
           self.skipTest("No database URL set")
       self.engine = sqlalchemy.create_engine(self.database_connection)
       self.connection = self.engine.connect()
       self.connection.execute("CREATE DATABASE testdb")

    def tearDown(self):
        self.connection.execute("DROP DATABASE testdb")
'''

'''
from unittest.mock import patch, MagicMock

@patch('mypackage.mymodule.pymysql')
def test(self, mock_sql):
    self.assertIs(mypackage.mymodule.pymysql, mock_sql)

    conn = Mock()
    mock_sql.connect.return_value = conn

    cursor      = MagicMock()
    mock_result = MagicMock()

    cursor.__enter__.return_value = mock_result
    cursor.__exit___              = MagicMock()

    conn.cursor.return_value = cursor

    connectDB()

    mock_sql.connect.assert_called_with(host='localhost',
                                        user='user',
                                        password='passwd',
                                        db='db')

    mock_result.execute.assert_called_with("sql request", ("user", "pass"))
'''

"""
