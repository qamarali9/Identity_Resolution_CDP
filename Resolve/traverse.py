from cassandra.cluster import Cluster
from config import *

def deterministic_ids_traversal(session,cdp_uid):
    uid_list_traverse = [cdp_uid]
    processed_identifier_dict ={cdp_uid:False}
    uid_identifier_list = []
    while uid_list_traverse :
        curr_uid = uid_list_traverse.pop(0)
        processed_identifier_dict[curr_uid] = True
        select_id_query = """SELECT *
        FROM identities
        WHERE cdp_uid = {};
        """.format(curr_uid)
        result = session.execute(select_id_query)
        for row in result:
            #print(row)
            if row.identifier_type != "path" and row.nature == "D":
                uid_identifier_list.append((row.cdp_uid,row.identifier)) 
            if row.identifier_type == "path" and row.nature == "S" and processed_identifier_dict.get(int(row.identifier),False) == False:
                uid_list_traverse.append(int(row.identifier))
                #print("uid_list_traverse : {}".format(uid_list_traverse))
        #break
    return list(set(uid_identifier_list))

def conflicting_ids_traversal(session,cdp_uid):
    uid_list_traverse = [cdp_uid]
    processed_identifier_dict ={cdp_uid:False}
    uid_identifier_list = []
    while uid_list_traverse :
        curr_uid = uid_list_traverse.pop(0)
        processed_identifier_dict[curr_uid] = True
        select_id_query = """SELECT *
        FROM identities
        WHERE cdp_uid = {};
        """.format(curr_uid)
        result = session.execute(select_id_query)
        for row in result:
            #print(row)
            if row.identifier_type != "path" and row.nature == "C":
                uid_identifier_list.append((row.cdp_uid,row.identifier)) 
            if row.identifier_type == "path" and processed_identifier_dict.get(int(row.identifier),False) == False:
                uid_list_traverse.append(int(row.identifier))
                #print("uid_list_traverse : {}".format(uid_list_traverse))
        #break
    return list(set(uid_identifier_list))

def events_traversal(session,uid_identifier_list):
    events_list = []
    for uid_identifier in uid_identifier_list:
        cdp_uid = uid_identifier[0]
        identifier = uid_identifier[1]
        query = """SELECT *
        FROM events
        WHERE cdp_uid = {};
        """.format(cdp_uid,identifier)
        result = session.execute(query)
        for row in result:
            if row.resolver_id == identifier:
                events_list.append(row.event)
    return events_list

def main(session,cdp_uid_to_traverse):
    uid_identifier_list = deterministic_ids_traversal(session,cdp_uid_to_traverse)
    print("Deterministic identifiers associated with cdp_uid {}:\n{} ".format(cdp_uid_to_traverse,uid_identifier_list))

    events_list = events_traversal(session,uid_identifier_list)
    print("Deterministic events associated with cdp_uid {}:\n{} ".format(cdp_uid_to_traverse,events_list))

    uid_identifier_list = conflicting_ids_traversal(session,cdp_uid_to_traverse)
    print("Conflicting identifiers associated with cdp_uid {}:\n{} ".format(cdp_uid_to_traverse,uid_identifier_list))

    events_list = events_traversal(session,uid_identifier_list)
    print("Conflicting events associated with cdp_uid {}:\n{} ".format(cdp_uid_to_traverse,events_list))

if __name__ == "__main__":
    cluster = Cluster()
    session = cluster.connect(CASSANDRA_KEYSPACE)

    main(session,1)
