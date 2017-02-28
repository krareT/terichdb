# Using TerichDB as MongoDB storage engine

## Compile

```shell
# your mongo dir is ~/mongo, your terak-db dir is ~/terichdb
# your terichdb binary package is /path/to/terichdb/package/
# now add TerichDB module to mongo
mkdir -p ~/mongo/src/mongo/db/modules/
ln -sf ~/terichdb/src/mongo_terichdb ~/mongo/src/mongo/db/modules/

# compile mongo
cd ~/mongo;
scons --disable-warnings-as-errors CPPPATH=/path/to/terichdb/package/include LIBPATH=/path/to/terichdb/package/lib
```

## Run mongodb with TerichDB storage engine
```shell
cd ~/mongo;
# MongoTerichDB needs create collection with schema before inserting data,
# when running mongodb testcases, it has no chance to create such collections,
# set these two env var as 1, MongoTerichDB will dynamicaly create a collection
# with _id being ObjectID type(fixed length field which length=12), and all other fields
# export MongoTerichDB_DynamicCreateCollection=1
# export MongoTerichDB_DynamicCreateIndex=1
mkdir tdb
./mongod --dbpath tdb --storageEngine TerarkSegDB
```

## Data Migration

1. Add content of [terark-mongo.js](../../tools/mongo/shell/terark-mongo.js) to ~/mongorc.js, which defined fuction `terarkCreateColl(dbname, collname, schemaFile)`.
   * Extra param `opt` is not used now
1. Using [Terark modified variety](https://github.com/Terark/variety) to deduce the schema of existing mongoDB colletions.
1. Calling `terarkCreateColl(dbname, collname, schemaFile)` to create a collection and its indices by the deduced schema.
   * Note: This is required before inserting data into mongoTerichDB!!
1. Using [mongodump/mongorestore](https://github.com/mongodb/mongo-tools) to copy data from existing mongoDB to mongoTerichDB.
   * You can insert data to mongoTerichDB by any other approaches
