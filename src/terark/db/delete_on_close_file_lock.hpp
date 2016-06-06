#ifndef __terark_db_delete_on_close_file_lock_hpp__
#define __terark_db_delete_on_close_file_lock_hpp__

#include <stdio.h>
#include <boost/filesystem.hpp>
#include "db_dll_decl.hpp"

namespace terark { namespace db {

class TERARK_DB_DLL DeleteOnCloseFileLock final {
	FILE* m_file;
	boost::filesystem::path m_fpath;
public:
	DeleteOnCloseFileLock(const boost::filesystem::path& fpath);
	~DeleteOnCloseFileLock();
	void closeDontDelete();
};

} } // namespace terark::db

#endif // __terark_db_delete_on_close_file_lock_hpp__
