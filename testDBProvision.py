import time as t
import configparser
import uuid
import sys

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

def createCollectionPT(collectionName, Database):
  databaseLink = "dbs/"+Database
  coll = {
          "id": collectionName,
          "indexingPolicy": {
              "indexingMode": "lazy",
              "automatic": True
          },
          "partitionKey": {
              "paths": [
                "/id"
              ],
              "kind": "Hash"
          }
         }
  try:
      collection = client.CreateCollection(databaseLink, coll )
      print('Collection with id \'{0}\' created'.format(collection['id']))
  except errors.DocumentDBError as e:
      if e.status_code == 409:
         print('A collection with id '+collectionName+' already exists')
      else: 
          print(e)
          raise errors.HTTPFailure(e.status_code)

def getCollectionRU(collectionName, DBid ):
  collection_link = 'dbs/' + DBid + '/colls/' + collectionName
  collection = client.ReadCollection(collection_link)
  # bad code ! assumes there is a return from the query
  offer = list(client.QueryOffers('SELECT * FROM c WHERE c.resource = \'{0}\''.format(collection['_self'])))
  # if offer is empty then there is no throughput specified for the collection and it is specified at the Database 
  # test this and return appropriate offer throughput
  if len(offer) == 0:
      return None
  elif len(offer) == 1:
      return offer[0]['content']['offerThroughput']
  else:
      print("unexpected number of offers returned")

def deleteDatabase(client, id):
        try:
           database_link = 'dbs/' + id
           client.DeleteDatabase(database_link)

           print('Database with id \'{0}\' was deleted'.format(id))

        except errors.DocumentDBError as e:
            if e.status_code == 404:
               print('A database with id \'{0}\' does not exist'.format(id))
            else: 
                raise errors.HTTPFailure(e.status_code)


config = configparser.ConfigParser()
config.read('config.ini')
host = config.get('OSTEST', 'CdbURI')
dbKey = config.get('OSTEST', 'CdbKey')
databaseId = config.get('OSTEST', 'CDbID')


config = {
    'ENDPOINT': host,
    'MASTERKEY': dbKey
}
client = document_client.DocumentClient(config['ENDPOINT'], {'masterKey': config['MASTERKEY']})

dbName = 'deldb_djb'
collectName = 'mojitos'
throughput = 11000

deleteDatabase(client, dbName)

createDatabasePT(client, dbName, throughput)
print("created database {}".format(dbName))

createCollectionPT( collectName, dbName)
    
collectDetails = getCollectionRU(collectName, dbName)


print( "Initially configured Capacity for database {0} = {1}."
    .format(dbName, getDatabaseRU(client, dbName)) )
r = setDatabaseRU(client, dbName, 10000)
print("After update configured Capacity for database {0} + {1}."
    .format(dbName, getDatabaseRU(client, dbName)) )

deleteDatabase(client, dbName)


