from cassandra.cluster import Cluster
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

def get_connection(config,keyspace):
    #logger.info("get_connection() called with params (config : {} , keyspace : {})".format(config,keyspace))
    try:
        cluster = Cluster(**config)
        session = cluster.connect()
        session.set_keyspace(keyspace)
        logger.info("Created a Cassandra session with params (config : {} , keyspace : {})".format(config,keyspace))
        return session
    except Exception as Exc:
        logger.error("(In get_connection()) Could not create a cassandra session with params (config : {} , keyspace : {})".format(config,keyspace))
        print(Exc)
        return None
