# Using TerarkDB as MongoDB storage engine

## Compile

```shell
# your mongo dir is ~/mongo, your terak-db dir is ~/terark-db
# your terark-db binary package is /path/to/terark-db/package/
# now add TerarkDB module to mongo
mkdir -p ~/mongo/src/mongo/db/modules/
ln -sf ~/terark-db/src/mongo_terarkdb ~/mongo/src/mongo/db/modules/

# compile mongo
cd ~/mongo;
scons CPPPATH=/path/to/terark-db/package/include LIBPATH=/path/to/terark-db/package/lib
```

## Data Migration

1. Add content of [terark-mongo.js](../../tools/mongo/shell/terark-mongo.js) to ~/mongorc.js, which defined fuction `terarkCreateColl(dbname, collname, schemaFile)`.
   * Extra param `opt` is not used now
1. Using [Terark modified variety](https://github.com/Terark/variety) to deduce the schema of existing mongoDB colletions.
1. Calling `terarkCreateColl(dbname, collname, schemaFile)` to create a collection and its indices by the deduced schema.
   * Note: This is required before inserting data into mongoTerarkDB!!
1. Using [mongodump/mongorestore](https://github.com/mongodb/mongo-tools) to copy data from existing mongoDB to mongoTerarkDB.
   * You can insert data to mongoTerarkDB by any other approaches
