# Using TerarkDB as MongoDB storage engine

## Compile

```shell
# add TerarkDB module to mongo
mkdir -p mongo/src/mongo/db/modules/
ln -sf ~/mongo-terarkdb mongo/src/mongo/db/modules/mongo-terarkdb
# compile mongo
cd mongo;
scons CPPPATH=/path/to/terark-db/package/include LIBPATH=/path/to/terark-db/package/lib
```
