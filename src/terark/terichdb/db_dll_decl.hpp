#ifndef __terichdb_dll_decl_hpp__
#define __terichdb_dll_decl_hpp__

#if defined(_MSC_VER)

#  if defined(TERICHDB_CREATE_DLL)
#    pragma warning(disable: 4251)
#    define TERICHDB_DLL __declspec(dllexport)      // creator of dll
#    if defined(_DEBUG) || !defined(NDEBUG)
#//	   pragma message("creating terichdb-d.lib")
#    else
#//	   pragma message("creating terichdb-r.lib")
#    endif
#  elif defined(TERICHDB_USE_DLL)
#    pragma warning(disable: 4251)
#    define TERICHDB_DLL __declspec(dllimport)      // user of dll
#    if defined(_DEBUG) || !defined(NDEBUG)
//#	   pragma comment(lib, "terichdb-d.lib")
#    else
//#	   pragma comment(lib, "terichdb-r.lib")
#    endif
#  else
#    define TERICHDB_DLL                            // static lib creator or user
#  endif

#else /* _MSC_VER */

#  define TERICHDB_DLL

#endif /* _MSC_VER */

#endif //__terichdb_dll_decl_hpp__
