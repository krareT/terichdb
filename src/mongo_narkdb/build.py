
def configure(conf, env):
    print("Configuring MongoNarkWritableDB storage engine module")
    if not conf.CheckCXXHeader("nark/fsa/fsa.hpp"):
        print("Could not find <nark/fsa/fsa.hpp>, required for MongoNarkWritableDB storage engine build.")
        env.Exit(1)
