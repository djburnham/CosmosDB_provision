""" Functions to create Cosmos DB Databases with provisioned storage and 
    to set and get the provisioned capacity of a database """
import pydocumentdb
import pydocumentdb.errors as errors
import pydocumentdb.document_client as document_client

def getDatabaseRU(client, databaseName):
    """Retrieve the provisioned capacity of a Cosmos DB database  """
    try:
        vdb = list(client.QueryDatabases
            ("SELECT * FROM d WHERE d.id = '{0}'".format(databaseName)) )
        if len(vdb) == 1:
            dbres = vdb[0]['_self']
            dboffr = list(client.QueryOffers
                ("SELECT * FROM o WHERE o.resource = '{0}'".format(dbres) ))
            if len(dboffr) == 1:
                return dboffr[0]['content']['offerThroughput']
            elif len(dboffr) == 0:
                # we didn't get any offers for the database
                return None
            else:
                # got more than one offer ?error condition
                raise RuntimeError("More than 1 offer found for resource")
        elif len(vdb) == 0 :
            raise RuntimeError("Database {0} not found ".format(databaseName))
        else:
            raise RuntimeError("More than 1 Database {0} found".format(databaseName)) 
            # this really shouldn't ever happen
    except errors.HTTPFailure as e:
        if e.status_code == 404:
                print('A database with id \'{0}\' does not exist'.format(databaseName))
        else: 
                raise errors.HTTPFailure(e.status_code)
 

def setDatabaseRU(client, databaseName, RUs):
    """Set the provisioned capacity of a Cosmos DB database  """
    if RUs < 10000 or RUs > 260000 or RUs%1000 != 0:
        raise ValueError(
            'RUs must be between 10000 and 260000 and a multiple of 1000')
    else:
        try:
            vdb = list(client.QueryDatabases
                ("SELECT * FROM d WHERE d.id = '{0}'".format(databaseName)) )
            if len(vdb) == 1:
                dbres = vdb[0]['_self']
                dboffr = list(client.QueryOffers
                    ("SELECT * FROM o WHERE o.resource = '{0}'".format(dbres) ))
                if len(dboffr) == 1:
                    dboffr[0]['content']['offerThroughput'] = RUs
                    offer = client.ReplaceOffer(dboffr[0]['_self'], dboffr[0])
                    return offer['content']['offerThroughput']

                elif len(dboffr) == 0:
                    # No offers for the database - collection provisioned? 
                    return None
                else:
                    # got more than one offer ?error condition
                    raise RuntimeError("More than 1 offer found for resource")
            elif len(vdb) == 0 :
                raise RuntimeError("Database {0} not found ".format(databaseName))
            else:
                raise RuntimeError("More than 1 Database {0} found".format(databaseName)) 
                # this really shouldn't ever happen
        except errors.HTTPFailure as e:
            if e.status_code == 404:
                print('A database with id \'{0}\' does not exist'.format(databaseName))
            else: 
                raise errors.HTTPFailure(e.status_code)  

def createDatabasePT(client, id, ruThroughput):
    """Create a CosmosDB Database with Provisioned Storage"""
    try:
        client.CreateDatabase({"id": id}, {"offerThroughput": ruThroughput})
        print('Database with id \'{0}\' created'.format(id))

    except errors.DocumentDBError as e:
        if e.status_code == 409:
            print('A database with id \'{0}\' already exists'.format(id))
            raise errors.HTTPFailure(e.status_code)
        elif ( e.status_code == 400 and 
            isinstance(e,pydocumentdb.errors.HTTPFailure) ):
            if 'throughput values between' in e.args[0]:
                print('Invalid throughput value {0}'.format(ruThroughput))
                raise errors.HTTPFailure(e.status_code)
            else:
                raise errors.HTTPFailure(e.status_code)
