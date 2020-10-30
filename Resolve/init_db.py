from cassandra.cluster import Cluster
from config import *
import logging

# create logger with __name__
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create file handler which logs even debug messages
fh = logging.FileHandler('log_identityResolution.log')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

def create_db(session):
    logger.info("create_db() called")
    try:
        replication_dict = {
                'class' : 'SimpleStrategy',
                'replication_factor' : 1
                }
        create_keyspace_query = """
        CREATE KEYSPACE {}
        WITH REPLICATION = {};
        """.format(CASSANDRA_KEYSPACE,replication_dict)
        session.execute(create_keyspace_query)
        session.set_keyspace(CASSANDRA_KEYSPACE)
        logger.info("Created keyspace {}".format(CASSANDRA_KEYSPACE))
    except Exception as Exc:
        logger.error("(In create_db()) Could not create keyspace - {}".format(CASSANDRA_KEYSPACE))
        print(Exc)

def create_table_identities(session):
    #logger.info("create_table_identities() called")
    try:
        create_table_query = """
        CREATE TABLE identities
        ( identifier varchar,
        identifier_type varchar,
        customer_id int,
        cdp_uid int,
        resolver_id varchar,
        resolver_id_type varchar,
        nature varchar,
        conflicting_ids set<text>,
        created_time timestamp,
        active boolean,
        PRIMARY KEY ((identifier, identifier_type, customer_id), cdp_uid, active)
        );
        """
        session.execute(create_table_query)
        logging.info("Created identities")
        create_index_query = """
        CREATE INDEX cdp_uid_idx
        ON identities(cdp_uid);
        """
        session.execute(create_index_query)
        logging.info("Created index cdp_uid_idx")
    except Exception as Exc:
        logger.error("(In create_table()) Could not create table - identities")
        print(Exc)

def create_table_events(session):
    #logger.info("create_table_events() called")
    try:
        create_table_query = """
        CREATE TABLE events
        ( cdp_uid int,
        event varchar,
        resolver_id varchar,
        created_time timestamp,
        active boolean,
        PRIMARY KEY ((event), cdp_uid, active)
        );
        """
        session.execute(create_table_query)
        logging.info("Created events")
        create_index_query = """
        CREATE INDEX events_cdp_uid_idx
        ON events(cdp_uid);
        """
        session.execute(create_index_query)
        logging.info("Created index events_cdp_uid_idx")
    except Exception as Exc:
        logger.error("(In create_table()) Could not create table - identities")
        print(Exc)

if __name__ == "__main__":
    cluster = Cluster()
    session = cluster.connect()
    create_db(session)
    create_table_identities(session)
    create_table_events(session)
