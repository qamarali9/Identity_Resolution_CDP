from cassandra.cluster import Cluster
from config import *
from id_generator import id_generator

def check_exists_and_return_row(session,identifier,identifier_type):
    """ Returns the entry (row) corresponding to identifier and identifier_type if the entry exists,
        otherwise returns None

        Keyword arguments:
        session -- the session object from cassandra connection
        identifier -- the identifier that needs to be searched
        identifier_type -- the type corresponding to the identifier that needs to be searched
    """
    query = """
    SELECT * FROM
    identities
    WHERE identifier = '{}'
    AND identifier_type = '{}'
    AND customer_id = {};
    """.format(identifier,identifier_type,CUSTOMER_ID)

    #print(query)

    results = session.execute(query)

    if results:
        return results.one()
    else:
        return None

def update_identity_deterministic(session,identifier,identifier_type,cdp_uid,resolver_id,resolver_id_type):
    """ Updates the entry for the identifier in the identities table for the deterministic case.

        Keyword arguments:
        session -- the session object from cassandra connection
        identifier -- the identifier that needs to be searched
        identifier_type -- the type corresponding to the identifier that needs to be searched
        cdp_uid -- the cdp_uid of the identifier
        resolver_id -- the resolver identifier for the current identifier
        resolver_id_type -- the type of the resolver identifier for the current identifier
    """
    return session.execute("""
    UPDATE
    identities
    SET nature = %(nature)s, resolver_id = %(resolver_id)s, resolver_id_type = %(resolver_id_type)s, conflicting_ids = %(conflicting_ids)s 
    WHERE identifier = '{}' AND identifier_type = '{}' AND customer_id = {} AND active={} AND cdp_uid={}
    IF EXISTS;
    """.format(identifier,identifier_type,CUSTOMER_ID,"true",cdp_uid), 
    {'nature':"D", 'resolver_id':resolver_id, 'resolver_id_type':resolver_id_type, 'conflicting_ids':None})

def update_identity_conflicting(session,identifier,identifier_type,cdp_uid,conflicting_ids_set,resolver_id_type):
    """ Updates the entry for the identifier in the identities table for the conflicting case.

        Keyword arguments:
        session -- the session object from cassandra connection
        identifier -- the identifier that needs to be searched
        identifier_type -- the type corresponding to the identifier that needs to be searched
        cdp_uid -- the cdp_uid of the identifier
        conflicting_ids_set -- the set of identifiers which corresponds to the conflicting resolution
        resolver_id_type -- the type of the conflicting resolvers. All of the conflicting resolvers will have 1 type
    """
    return session.execute("""
    UPDATE
    identities
    SET nature = %(nature)s, conflicting_ids = %(conflicting_ids)s, resolver_id_type = %(resolver_id_type)s
    WHERE identifier = '{}' AND identifier_type = '{}' AND customer_id = {} AND active={} AND cdp_uid={}
    IF EXISTS;
    """.format(identifier,identifier_type,CUSTOMER_ID,"true",cdp_uid), 
    {'nature':"C", 'conflicting_ids':conflicting_ids_set, 'resolver_id_type':resolver_id_type})

def update_weak_to_strong_path(session,uid1,uid2):
    """ Updates a weak path to a strong path when a conflicting identifier changes from 'conflicting' to 'deterministic'.

        Keyword arguments:
        session -- the session object from cassandra connection
        uid1 -- the first uid of the 'weak' path that has to be changed to 'strong'. This is stored as identifier
                in the corresponding entry of the path.
        uid1 -- the second uid of the 'weak' path that has to be changed to 'strong'. This is stored as cdp_uid
                in the corresponding entry of the path.
    """
    return session.execute("""
    UPDATE
    identities
    SET nature = %(nature)s
    WHERE identifier = '{}' AND identifier_type = '{}' AND customer_id = {} AND active={} AND cdp_uid={}
    IF EXISTS;
    """.format(str(uid1), "path", CUSTOMER_ID, "true", uid2), 
    {'nature':"S"})

def insert_strong_path(session,uid1,uid2):
    """ Inserts a strong path entry in the identities table.

        Keyword arguments:
        session -- the session object from cassandra connection
        uid1 -- the first uid of the 'strong' path that has to be inserted. This is stored as identifier
                in the corresponding entry of the path.
        uid1 -- the second uid of the 'strong' path that has to be inserted. This is stored as cdp_uid
                in the corresponding entry of the path.
    """
    insert_query = """
    INSERT INTO
    identities
    (identifier, identifier_type, customer_id, cdp_uid, active, conflicting_ids, created_time, nature, resolver_id, resolver_id_type)
    VALUES
    ('{}', '{}', {}, {}, {}, {},toTimestamp(now()),'{}',{},{})
    IF NOT EXISTS; 
    """.format(str(uid1), "path", CUSTOMER_ID, uid2, "true", 'null', "S", 'null', 'null')
    # toTimestamp(now())
    #print(insert_query)

    results = session.execute(insert_query)
    #print(results)


def insert_weak_path(session,uid1,uid2):
    """ Inserts a weak path entry in the identities table.

        Keyword arguments:
        session -- the session object from cassandra connection
        uid1 -- the first uid of the 'weak' path that has to be inserted. This is stored as identifier
                in the corresponding entry of the path.
        uid1 -- the second uid of the 'weak' path that has to be inserted. This is stored as cdp_uid
                in the corresponding entry of the path.
    """
    insert_query = """
    INSERT INTO
    identities
    (identifier, identifier_type, customer_id, cdp_uid, active, conflicting_ids, created_time, nature, resolver_id, resolver_id_type)
    VALUES
    ('{}', '{}', {}, {}, {}, {},toTimestamp(now()),'{}',{},{})
    IF NOT EXISTS;  
    """.format(str(uid1), "path", CUSTOMER_ID, uid2, "true", 'null', "W", 'null', 'null')
    # toTimestamp(now())
    #print(insert_query)

    results = session.execute(insert_query)
    #print(results)

def insert_identifier_with_uid_deterministic(session,identifier,identifier_type,cdp_uid,resolver_id,resolver_id_type):
    """ Inserts an identifier which is deterministic.

        Keyword arguments:
        session -- the session object from cassandra connection
        identifier -- the identifier that needs to be inserted
        identifier_type -- the type corresponding to the identifier that needs to be inserted
        cdp_uid -- the cdp_uid associated with this identifier (which is the cdp_uid of its resolver)
        resolver_id -- the resolver identifier for the current identifier
        resolver_id_type -- the type of the resolver identifier for the current identifier
    """
    insert_query = """
    INSERT INTO
    identities
    (identifier, identifier_type, customer_id, cdp_uid, active, conflicting_ids, created_time, nature, resolver_id, resolver_id_type)
    VALUES
    ('{}', '{}', {}, {}, {}, {},toTimestamp(now()),'{}','{}','{}' )
    IF NOT EXISTS;
    """.format(identifier, identifier_type, CUSTOMER_ID, cdp_uid, "true", 'null', "D", resolver_id, resolver_id_type)
    # toTimestamp(now())
    #print(insert_query)

    results = session.execute(insert_query)
    #print(results)

    row = check_exists_and_return_row(session,identifier,identifier_type)
    return row


def insert_identifier_with_uid_conflicting(session,identifier,identifier_type,cdp_uid,conflicting_ids_set,conflicting_resolver_id_type):
    """ Inserts an identifier which is conflicting.

        Keyword arguments:
        session -- the session object from cassandra connection
        identifier -- the identifier that needs to be inserted
        identifier_type -- the type corresponding to the identifier that needs to be inserted
        cdp_uid -- the cdp_uid associated with this identifier (which is the cdp_uid of resolver for the current event)
        conflicting_ids_set -- the set of identifiers which corresponds to the conflicting resolution
        conflicting_resolver_id_type -- the type of the conflicting resolvers. All of the conflicting resolvers will have 1 type
    """
    insert_query = """
    INSERT INTO
    identities
    (identifier, identifier_type, customer_id, cdp_uid, active, conflicting_ids, created_time, nature, resolver_id, resolver_id_type)
    VALUES
    ('{}', '{}', {}, {}, {}, {},toTimestamp(now()),'{}','{}','{}' )
    IF NOT EXISTS;
    """.format(identifier, identifier_type, CUSTOMER_ID, cdp_uid, "true", conflicting_ids_set, "C", next(iter(conflicting_ids_set)), conflicting_resolver_id_type)
    # toTimestamp(now())
    #print(insert_query)

    results = session.execute(insert_query)
    #print(results)

    row = check_exists_and_return_row(session,identifier,identifier_type)
    return row


def insert_new_identity(session,result_dict):
    """ Inserts a new entry in the identities table (the identifier is its own resolver and is deterministic).

        Keyword arguments:
        session -- the session object from cassandra connection
        result_dict --  the dictionary corresponding to the identifier (it gets resolved by itself and is
                        the most strictest for the current row/event)
                        Example of result_dict -- {"most_strict_val":5,"most_strict_resolver":C1,
                        "most_strict_resolver_type":"cust", "most_strict_resolver_exists":True, "row":RowObject()}
    """
    uid = id_generator.gen_id()
    #print(uid)
    insert_query = """
    INSERT INTO
    identities
    (identifier, identifier_type, customer_id, cdp_uid, active, conflicting_ids, created_time, nature, resolver_id, resolver_id_type)
    VALUES
    ('{}', '{}', {}, {}, {}, {},toTimestamp(now()),'{}','{}','{}' )
    IF NOT EXISTS;
    """.format(result_dict["most_strict_resolver"], result_dict["most_strict_resolver_type"], CUSTOMER_ID, uid, "true", 'null', "D",result_dict["most_strict_resolver"], result_dict["most_strict_resolver_type"])
    # toTimestamp(now())
    #print(insert_query)

    results = session.execute(insert_query)
    #print(results)

    #row = check_exists_and_return_row(result_dict["most_strict_resolver"], result_dict["most_strict_resolver_type"])
    return results

def insert_new_event(session,event,event_resolver_row):
    """ Inserts a new event in the events table.

        Keyword arguments:
        session -- the session object from cassandra connection
        event -- the event to be inserted
        event_resolver_row -- the row in the identities table of the resolver identifier for the current event
    """
    #print(uid)
    insert_query = """
    INSERT INTO
    events
    (event, cdp_uid, active, created_time, resolver_id)
    VALUES
    ('{}', {}, {}, toTimestamp(now()), '{}')
    IF NOT EXISTS;  
    """.format(event, event_resolver_row.cdp_uid, "true", event_resolver_row.identifier)
    # toTimestamp(now())
    #print(insert_query)

    results = session.execute(insert_query)
    #print(results)

    #row = check_exists_and_return_row(result_dict["most_strict_resolver"], result_dict["most_strict_resolver_type"])
    return results

def find_resolver_for_event(details_dict):
    """Find the resolver identifier (i.e, most strictness identifier with which the current event will be resolved)
       Based on the logic of finding maximum element in an array, starting with a sentinel value(-1). Returns a dicionary 
       of the resolver identifier. Do note that there will be one resolver for the current event. 
       
       Keyword arguments:
       details_dict -- the dictionary corresponding to all the identifiers with the current event (current row)
                       Example of details_dict -- {"C1":{"type":"cust", "row":RowObject()}, ...}

       Returns:
       result_dict --  the dictionary corresponding to the resolver identifier for the current event
                       Example of result_dict -- {"most_strict_val":5,"most_strict_resolver":C1,
                       "most_strict_resolver_type":"cust", "most_strict_resolver_exists":True, "row":RowObject()}

    """
    result_dict = {}
    result_dict["most_strict_val"] = -1
    result_dict["most_strict_resolver"] = None
    result_dict["most_strict_resolver_type"] = None
    result_dict["most_strict_resolver_exists"] = None
    result_dict["row"] = None

    for identifier in details_dict:
        row = details_dict[identifier]["row"]
        if(row==None):
            curr_resolver = identifier
            curr_type = details_dict[identifier]["type"]
            curr_val = STRICTNESS_DICT[curr_type]
            curr_exists = False
            curr_row = None
        else:
            curr_resolver = row.identifier
            curr_type = row.identifier_type
            curr_val = STRICTNESS_DICT[curr_type]
            curr_exists = True
            curr_row = row
		
        if(result_dict["most_strict_val"] < curr_val):
            result_dict["most_strict_val"] = curr_val
            result_dict["most_strict_resolver"] = curr_resolver
            result_dict["most_strict_resolver_type"] = curr_type
            result_dict["most_strict_resolver_exists"] = curr_exists
            result_dict["row"] = curr_row

    return result_dict

def find_resolver(details_dict):
    """Find the resolver identifier(s) (i.e, top-most strictness in the details_dict) with which
       the other identifiers of the current row will get resolved. Based on the logic of finding maximum element(s) 
       in an array, starting with a sentinel value(-1). Returns a dicionary of all the resolver identifiers (having
       the same strictness). Do note the differnce between results_dict and result_dict 
       
       Keyword arguments:
       details_dict -- the dictionary corresponding to all the identifiers with the current event (current row)
                       Example of details_dict -- {"C1":{"type":"cust", "row":RowObject()}, ...}

       Returns:
       results_dict -- A dicionary of all the resolver identifiers (having the same strictness).
                       Example of results_dict -- {"C1":{"most_strict_val":5,"most_strict_resolver":C1,
                       "most_strict_resolver_type":"cust", "most_strict_resolver_exists":True, "row":RowObject()}, 
                       "C2":{"most_strict_val":5,"most_strict_resolver":C2,
                       "most_strict_resolver_type":"cust", "most_strict_resolver_exists":False, "row":None}, ... }

    """
    results_dict = {}
    result_dict = {}
    result_dict["most_strict_val"] = -1
    result_dict["most_strict_resolver"] = None
    result_dict["most_strict_resolver_type"] = None
    result_dict["most_strict_resolver_exists"] = None
    result_dict["row"] = None

    for identifier in details_dict:
        row = details_dict[identifier]["row"]
        if(row==None):# if an entry does not exist for the current identifier
            curr_resolver = identifier
            curr_type = details_dict[identifier]["type"]
            curr_val = STRICTNESS_DICT[curr_type]
            curr_exists = False
            curr_row = None
            curr_nature = None
        else:
            curr_resolver = row.resolver_id # resolver identifier for current identifier, fetched from its RowObject()
            curr_type = row.resolver_id_type
            curr_val = STRICTNESS_DICT[curr_type]
            curr_exists = True
            curr_row = row
            curr_nature = row.nature
		
        if(result_dict["most_strict_val"] < curr_val):
            results_dict =  {}
            if curr_nature != "C":
                results_dict[curr_resolver] = {}
                results_dict[curr_resolver]["most_strict_val"] = curr_val
                results_dict[curr_resolver]["most_strict_resolver"] = curr_resolver
                results_dict[curr_resolver]["most_strict_resolver_type"] = curr_type
                results_dict[curr_resolver]["most_strict_resolver_exists"] = curr_exists
                results_dict[curr_resolver]["row"] = curr_row
                result_dict = results_dict[curr_resolver]
            else:
                for conflicting_resolver in row.conflicting_ids:
                    results_dict[conflicting_resolver] = {}
                    results_dict[conflicting_resolver]["most_strict_val"] = curr_val
                    results_dict[conflicting_resolver]["most_strict_resolver"] = conflicting_resolver
                    results_dict[conflicting_resolver]["most_strict_resolver_type"] = curr_type
                    results_dict[conflicting_resolver]["most_strict_resolver_exists"] = curr_exists
                    results_dict[conflicting_resolver]["row"] = curr_row
                    result_dict = results_dict[curr_resolver]

        elif(result_dict["most_strict_val"] == curr_val):
            if curr_nature != "C":
                results_dict[curr_resolver] = {}
                results_dict[curr_resolver]["most_strict_val"] = curr_val
                results_dict[curr_resolver]["most_strict_resolver"] = curr_resolver
                results_dict[curr_resolver]["most_strict_resolver_type"] = curr_type
                results_dict[curr_resolver]["most_strict_resolver_exists"] = curr_exists
                results_dict[curr_resolver]["row"] = curr_row
                result_dict = results_dict[curr_resolver]
            else:
                for conflicting_resolver in row.conflicting_ids:
                    results_dict[conflicting_resolver] = {}
                    results_dict[conflicting_resolver]["most_strict_val"] = curr_val
                    results_dict[conflicting_resolver]["most_strict_resolver"] = conflicting_resolver
                    results_dict[conflicting_resolver]["most_strict_resolver_type"] = curr_type
                    results_dict[conflicting_resolver]["most_strict_resolver_exists"] = curr_exists
                    results_dict[conflicting_resolver]["row"] = curr_row
                    result_dict = results_dict[curr_resolver]

        #print("After identifier : {} -- results_dict : {}".format(identifier,results_dict))

    return results_dict

def resolve_entries_conflicting(session,results_dict,details_dict):
    """ Resolve identifiers for the conflicting case -- Create entries (update or insert) for the identifiers in details_dict
        based on the resolvers in results_dict when there are more than one resolvers, i.e, the sceanrio is conflicting.

        Keyword arguments:
        session -- the session object from cassandra connection
        results_dict -- the dictionary corresponding to the resolver identifiers
                        Example of results_dict -- {"C1":{"most_strict_val":5,"most_strict_resolver":C1,
                        "most_strict_resolver_type":"cust", "most_strict_resolver_exists":True, "row":RowObject()}, 
                        "C2":{"most_strict_val":5,"most_strict_resolver":C2,
                        "most_strict_resolver_type":"cust", "most_strict_resolver_exists":False, "row":None}, ... }
        details_dict -- the dictionary corresponding to all the identifiers with the current event (current row)
                        Example of details_dict -- {"C1":{"type":"cust", "row":RowObject()}, ...}
    """
    for identifier in details_dict:
        # When the identifier already has an entry in the identities table, in this case, the 'nature' of the ... 
        # ... identifier becomes conflicting and the conflicting_ids have to be inserted/updated and the appropriate ...
        # ... weak paths have to be inserted.
        if details_dict[identifier]["row"] != None and identifier not in results_dict:
            conflicting_ids_set = set(key for key in results_dict)
            for key in results_dict:
                resolver_id_type = results_dict[key]["most_strict_resolver_type"]
                break
            if details_dict[identifier]["row"].nature == "C" and resolver_id_type!=details_dict[identifier]["row"].resolver_id_type:
                for conflicting_resolver in details_dict[identifier]["row"].conflicting_ids:
                    row_conflicting_resolver = check_exists_and_return_row(session,conflicting_resolver,details_dict[identifier]["row"].resolver_id_type)
                    update_identity_conflicting(session,conflicting_resolver,row_conflicting_resolver.identifier_type,row_conflicting_resolver.cdp_uid,conflicting_ids_set,resolver_id_type)
                    for identifier2 in results_dict:
                        if(row_conflicting_resolver.cdp_uid!=results_dict[identifier2]["row"].cdp_uid):
                            insert_weak_path(session,row_conflicting_resolver.cdp_uid,results_dict[identifier2]["row"].cdp_uid)
                            insert_weak_path(session,results_dict[identifier2]["row"].cdp_uid,row_conflicting_resolver.cdp_uid)
            elif details_dict[identifier]["row"].nature == "D" and resolver_id_type!=details_dict[identifier]["row"].resolver_id_type:
                deterministic_resolver = details_dict[identifier]["row"].resolver_id
                row_deterministic_resolver = check_exists_and_return_row(session,deterministic_resolver,details_dict[identifier]["row"].resolver_id_type)
                update_identity_conflicting(session,deterministic_resolver,row_deterministic_resolver.identifier_type,row_deterministic_resolver.cdp_uid,conflicting_ids_set,resolver_id_type)
                for identifier2 in results_dict:
                    if(row_deterministic_resolver.cdp_uid!=results_dict[identifier2]["row"].cdp_uid):
                        insert_weak_path(session,row_deterministic_resolver.cdp_uid,results_dict[identifier2]["row"].cdp_uid)
                        insert_weak_path(session,results_dict[identifier2]["row"].cdp_uid,row_deterministic_resolver.cdp_uid)
            update_identity_conflicting(session,identifier,details_dict[identifier]["type"],details_dict[identifier]["row"].cdp_uid,conflicting_ids_set,resolver_id_type)
            for identifier2 in results_dict:
                if(details_dict[identifier]["row"].cdp_uid!=results_dict[identifier2]["row"].cdp_uid):
                    insert_weak_path(session,details_dict[identifier]["row"].cdp_uid,results_dict[identifier2]["row"].cdp_uid)
                    insert_weak_path(session,results_dict[identifier2]["row"].cdp_uid,details_dict[identifier]["row"].cdp_uid)
        # When the identifier doesnot have an entry in the identities table, in this case, the appropriate entry ... 
        # ... has to be made for the identifier in the identities table. The uid for this entry will be the uid which ...
        # ... corresponds to the resolver for the current event.
        elif details_dict[identifier]["row"] == None and identifier not in results_dict:
            resolver_dict_tmp = find_resolver_for_event(details_dict)
            uid_to_insert = results_dict[resolver_dict_tmp["most_strict_resolver"]]["row"].cdp_uid
            insert_result = insert_identifier_with_uid_deterministic(session,identifier,details_dict[identifier]["type"],uid_to_insert,resolver_dict_tmp["most_strict_resolver"],resolver_dict_tmp["most_strict_resolver_type"])

            # conflicting_ids_set = set(key for key in results_dict)
            # for identifier2 in results_dict:
            #     conflicting_resolver_id_type = results_dict[identifier2]["most_strict_resolver_type"]
            #     break
            # insert_result = insert_identifier_with_uid_conflicting(session,identifier,details_dict[identifier]["type"],uid_to_insert,conflicting_ids_set,conflicting_resolver_id_type)
            #print(insert_result)

def resolve_entries_deterministic(session,results_dict,details_dict):
    """ Resolve identifiers for the deterministic case -- Create entries (update or insert) for the identifiers in
        details_dict based on the resolver in results_dict (there will be one resolver in results_dict in this case).

        Keyword arguments:
        session -- the session object from cassandra connection
        results_dict -- the dictionary corresponding to the resolver identifier (will have one entry)
                        Example of results_dict -- {"C1":{"most_strict_val":5,"most_strict_resolver":C1,
                        "most_strict_resolver_type":"cust", "most_strict_resolver_exists":True, "row":RowObject()}}
        details_dict -- the dictionary corresponding to all the identifiers with the current event (current row)
                        Example of details_dict -- {"C1":{"type":"cust", "row":RowObject()}, ...}
    """
    for identifier in results_dict:
        result_single_dict = results_dict[identifier]
        break
    for identifier in details_dict:
        # When the identifier already has an entry in the identities table, in this case, the 'nature' of the ... 
        # ... identifier becomes deterministic. If the identifier was conflicting all the identifiers in its set of ...
        # ... conflicting_ids have to be made deterministic with the current resolver and the weak paths  ...
        # ... have to be changed to strong paths. If the identifier was deterministic its resolver identifier ...
        # ... has to be resolved with the current resolver. Then the entry for the current identifier has to be ...
        # ... updated appropriately with the current resolver and the strong paths have to be inserted.
        if details_dict[identifier]["row"] != None and identifier not in results_dict:
            #print("\n\n----Here----\n\nidentifier:{}\nresults_dict={}".format(identifier,results_dict))
            if details_dict[identifier]["row"].nature == "C":
                for conflicting_resolver in details_dict[identifier]["row"].conflicting_ids:
                    row_conflicting_resolver = check_exists_and_return_row(session,conflicting_resolver,details_dict[identifier]["row"].resolver_id_type)
                    update_identity_deterministic(session,conflicting_resolver,row_conflicting_resolver.identifier_type,row_conflicting_resolver.cdp_uid,result_single_dict["most_strict_resolver"],result_single_dict["most_strict_resolver_type"])
                    update_weak_to_strong_path(session,row_conflicting_resolver.cdp_uid,details_dict[identifier]["row"].cdp_uid)
                    update_weak_to_strong_path(session,details_dict[identifier]["row"].cdp_uid,row_conflicting_resolver.cdp_uid)
            elif details_dict[identifier]["row"].nature == "D":
                deterministic_resolver = details_dict[identifier]["row"].resolver_id
                row_deterministic_resolver = check_exists_and_return_row(session,deterministic_resolver,details_dict[identifier]["row"].resolver_id_type)
                update_identity_deterministic(session,deterministic_resolver,row_deterministic_resolver.identifier_type,row_deterministic_resolver.cdp_uid,result_single_dict["most_strict_resolver"],result_single_dict["most_strict_resolver_type"])
                if(row_deterministic_resolver.cdp_uid!=result_single_dict["row"].cdp_uid):
                    insert_strong_path(session,row_deterministic_resolver.cdp_uid,result_single_dict["row"].cdp_uid)
                    insert_strong_path(session,result_single_dict["row"].cdp_uid,row_deterministic_resolver.cdp_uid)
            update_identity_deterministic(session,identifier,details_dict[identifier]["type"],details_dict[identifier]["row"].cdp_uid,result_single_dict["most_strict_resolver"],result_single_dict["most_strict_resolver_type"])
            if(details_dict[identifier]["row"].cdp_uid!=result_single_dict["row"].cdp_uid):
                insert_strong_path(session,details_dict[identifier]["row"].cdp_uid,result_single_dict["row"].cdp_uid)
                insert_strong_path(session,result_single_dict["row"].cdp_uid,details_dict[identifier]["row"].cdp_uid)
        # When the identifier doesnot have an entry in the identities table, in this case, the appropriate entry ... 
        # ... has to be made for the identifier in the identities table. The uid for this entry will be the uid which ...
        # ... corresponds to the uid of the current event.
        elif details_dict[identifier]["row"] == None and identifier not in results_dict:
            insert_result = insert_identifier_with_uid_deterministic(session,identifier,details_dict[identifier]["type"],result_single_dict["row"].cdp_uid,result_single_dict["row"].identifier,result_single_dict["row"].identifier_type)
            #print(insert_result)

def create_events_entry(session,event_resolver_dict, event):
    """ Create entry in the events table for the current event.

        Keyword arguments:
        session -- the session object from cassandra connection
        event_resolver_dict -- the dictionary corresponding to the resolver identifier for the current event
                   Example of event_resolver_dict -- {"most_strict_val":5,"most_strict_resolver":C1,
                   "most_strict_resolver_type":"cust", "most_strict_resolver_exists":True, "row":RowObject()}
        event -- the current event for which the entry has to be made
    """    
    event_resolver_row = check_exists_and_return_row(session,event_resolver_dict["most_strict_resolver"],event_resolver_dict["most_strict_resolver_type"])
    insert_new_event(session,event,event_resolver_row)

def create_entries(session,results_dict, details_dict):
    """ Create entries in the identities table (update or insert) based on the resolution

        Keyword arguments:
        session -- the session object from cassandra connection
        results_dict -- the dictionary corresponding to the resolver identifiers
                        Example of resolver_dict -- {"C1":{"most_strict_val":5,"most_strict_resolver":C1,
                        "most_strict_resolver_type":"cust", "most_strict_resolver_exists":True, "row":RowObject()}, 
                        "C1":{"most_strict_val":5,"most_strict_resolver":C2,
                        "most_strict_resolver_type":"cust", "most_strict_resolver_exists":False, "row":None}, ... }
        details_dict -- the dictionary corresponding to all the identifiers with the current event (current row)
                        Example of details_dict -- {"C1":{"type":"cust", "row":RowObject()}, ...}
    """
    # Create entries in the identities table for the resolver identifiers if the entry doesnot exist
    for resolver in results_dict:
        if(results_dict[resolver]["most_strict_resolver_exists"]==False):
            insert_new_identity(session,results_dict[resolver])
            results_dict[resolver]["row"] = check_exists_and_return_row(session,resolver,results_dict[resolver]["most_strict_resolver_type"])
        # insert resolver
        # compare resolver with each of the identifiers and perform appropriate inserts
        # pass
    if len(results_dict)>1:# if there are more than one resolvers, then it is conflicting case...
        resolve_entries_conflicting(session,results_dict, details_dict)
    else:# ... else it is deterministic case
        #print(results_dict)
        resolve_entries_deterministic(session,results_dict, details_dict)
    
    return

def main(session,input_dict):# 
    """ The main function of the resolution. Here is where the fun begins...

        Keyword arguments:
        session -- the session object from cassandra connection
        input_dict -- the dictionary corresponding to the current event and it's identifier i.e, the current
                      row in the sheet; An example input_dict : {"cust":"C1","email":"em1", "event":"E1"}
    """
    # Extract details of the identifier from the inout_dict along with events
    details_dict = {}# Example : {"C1":{"type":"cust", "row":RowObject()}, ...}
    for type_key in input_dict:
        if(type_key != "event"):
            if type(input_dict[type_key])!=list:
                identifier = input_dict[type_key]
                # get the row for the current identifier if it exists, otherwise None
                row = check_exists_and_return_row(session,identifier,type_key)
                details_dict[identifier] = {"type":type_key, "row":row}
            else:# if there are multiple identifiers of the same type for the current event
                for identifier_element in input_dict[type_key]:
                    identifier = identifier_element
                    row = check_exists_and_return_row(session,identifier,type_key)
                    details_dict[identifier] = {"type":type_key, "row":row}
        else:#extract the event
            event = input_dict[type_key]

    #print(details_dict)
    # find the resolver identifier(s) (i.e, top-most priority in the current row) with which...
    # ... the other identifiers of the current row will get resolved
    resolvers_dict = find_resolver(details_dict)
    """Example of resolvers_dict -- {"C1":{"most_strict_val":5,"most_strict_resolver":C1,
    "most_strict_resolver_type":"cust", "most_strict_resolver_exists":True, "row":RowObject()}, 
    "C2":{"most_strict_val":5,"most_strict_resolver":C2,
    "most_strict_resolver_type":"cust", "most_strict_resolver_exists":False, "row":None}, ... }"""
    #print(resolvers_dict)
    
    # create entries in the identities table (update or insert) based on the resolution
    create_entries(session,resolvers_dict,details_dict)

    # find the resolver identifier for the current event (there will be only one)
    event_resolver_dict = find_resolver_for_event(details_dict)
    """Example of event_resolver_dict -- {"most_strict_val":5,"most_strict_resolver":C1,
    "most_strict_resolver_type":"cust", "most_strict_resolver_exists":True, "row":RowObject()}"""
    #print("event_resolver_dict : {}".format(event_resolver_dict))
    
    # create entry in the events table for the current event based on its resolver
    create_events_entry(session,event_resolver_dict, event)

if __name__ == "__main__":
    # if getting run as a script, establish a cassandra connection and start from main()...
    cluster = Cluster()
    session = cluster.connect(CASSANDRA_KEYSPACE)

    #create_strictness_dict()
    #strictness_dict = STRICTNESS_DICTIONARY
    #main(session,{"cust":"C7", "event":"E15"})

    # sample_result_dict={}
    # sample_result_dict["most_strict_val"] = 4
    # sample_result_dict["most_strict_resolver"] = "em1"
    # sample_result_dict["most_strict_resolver_type"] = "email"
    # sample_result_dict["most_strict_resolver_exists"] = False
    
    # insert_new_identity(session,sample_result_dict)

    # check_row = check_exists_and_return_row(session,"em1","email")
    # print(check_row)
    # print(check_row.cdp_uid)

    #print(update_identity_conflicting(session,"em1","email",7,set(["C1","C2"])))

    #print(insert_weak_path(session,11,13))

    # sample_details_dict={}
    # sample_details_dict["C1"] = {}
    # sample_details_dict["C1"]["type"] = "cust"
    # sample_details_dict["C1"]["row"] = check_exists_and_return_row(session,"C1","cust")
    # sample_details_dict["em1"] = {}
    # sample_details_dict["em1"]["type"] = "email"
    # sample_details_dict["em1"]["row"] = check_exists_and_return_row(session,"em1","email")
    # print(sample_details_dict)
    # print(find_resolver_for_event(sample_details_dict))
    # sample_result_dict = find_resolver(sample_details_dict)
    # print(sample_result_dict)
    # create_entries(session,sample_result_dict,sample_details_dict)

    insert_identifier_with_uid_conflicting(session,"em15","email",8,set(["C1","C2"]),"cust")