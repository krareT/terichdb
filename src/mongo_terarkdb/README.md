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
