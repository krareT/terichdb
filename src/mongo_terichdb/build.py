
def configure(conf, env):
    print("Configuring MongoTerarkWritableDB storage engine module")
    if not conf.CheckCXXHeader("terark/terichdb/db_table.hpp"):
        print("Could not find <terark/terichdb/db_table.hpp>, required for MongoTerarkWritableDB storage engine build.")
        env.Exit(1)
