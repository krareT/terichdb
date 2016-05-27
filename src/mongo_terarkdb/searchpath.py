
# mongodb and scons is shit of shit
# fuck fuck!!!!
# this is not used any more
# just for fucking mongodb & scons

AddOption('--terarkdb-home',
    dest='terarkdb-home',
    type='string',
    nargs=1,
    action='store',
    metavar='DIR',
    help='terarkdb installation home directory')

AddOption('--terarkdb-include',
    dest='terarkdb-include',
    type='string',
    nargs=1,
    action='store',
    metavar='DIR',
    help='terarkdb include directory')

AddOption('--terarkdb-lib',
    dest='terarkdb-lib',
    type='string',
    nargs=1,
    action='store',
    metavar='DIR',
    help='terarkdb static/dynamic lib directory')

def addsearchpath(env):
    tlibPath = ''
    if GetOption('terarkdb-home'):
        nkEnv.Append(CPPPATH=[GetOption('terarkdb-home') + '/include'])
        nkEnv.Append(LIBPATH=[GetOption('terarkdb-home') + 'lib'])
        tlibPath = GetOption('terarkdb-home') + '/lib'

    if GetOption('terarkdb-include'):
        nkEnv.Append(CPPPATH=[GetOption('terarkdb-include')])

    print("tlibPath=" + tlibPath + " , fucking mongodb&scons bitch can not add LIBPATH")

    if GetOption('terarkdb-lib'):
        nkEnv.Append(LIBPATH=[GetOption('terarkdb-lib')])
        tlibPath = GetOption('terarkdb-lib')
