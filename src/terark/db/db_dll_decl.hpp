#ifndef __terark_db_dll_decl_hpp__
#define __terark_db_dll_decl_hpp__

#if defined(_MSC_VER)

#  if defined(TERARK_DB_CREATE_DLL)
#    pragma warning(disable: 4251)
#    define TERARK_DB_DLL __declspec(dllexport)      // creator of dll
#    if defined(_DEBUG) || !defined(NDEBUG)
#//	   pragma message("creating terark-db-d.lib")
#    else
#//	   pragma message("creating terark-db-r.lib")
#    endif
#  elif defined(TERARK_DB_USE_DLL)
#    pragma warning(disable: 4251)
#    define TERARK_DB_DLL __declspec(dllimport)      // user of dll
#    if defined(_DEBUG) || !defined(NDEBUG)
//#	   pragma comment(lib, "terark-db-d.lib")
#    else
//#	   pragma comment(lib, "terark-db-r.lib")
#    endif
#  else
#    define TERARK_DB_DLL                            // static lib creator or user
#  endif

#else /* _MSC_VER */

#  define TERARK_DB_DLL

#endif /* _MSC_VER */

#endif //__terark_db_dll_decl_hpp__
