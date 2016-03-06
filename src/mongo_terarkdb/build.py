
def configure(conf, env):
    print("Configuring MongoTerarkWritableDB storage engine module")
    if not conf.CheckCXXHeader("terark/fsa/fsa.hpp"):
        print("Could not find <terark/fsa/fsa.hpp>, required for MongoTerarkWritableDB storage engine build.")
        env.Exit(1)
