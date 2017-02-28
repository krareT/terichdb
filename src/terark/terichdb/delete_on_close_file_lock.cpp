#include "delete_on_close_file_lock.hpp"
#include <terark/io/FileStream.hpp>

namespace terark { namespace terichdb {

DeleteOnCloseFileLock::DeleteOnCloseFileLock(const boost::filesystem::path& fpath)
  : m_fpath(fpath) {
	const std::string& strFpath = fpath.string();
	m_file = fopen(strFpath.c_str(), "w");
	if (nullptr == m_file) {
		FileStream::ThrowOpenFileException(strFpath.c_str(), "w");
	}
}

DeleteOnCloseFileLock::~DeleteOnCloseFileLock() {
	if (nullptr == m_file) {
		return;
	}
	fclose(m_file);
	m_file = nullptr;
	try {
		boost::filesystem::remove(m_fpath);
	}
	catch (const std::exception& ex) {
		fprintf(stderr, "ERROR: remove(%s) = %s\n"
			, m_fpath.string().c_str(), ex.what());
	}
}

void DeleteOnCloseFileLock::closeDontDelete() {
	assert(nullptr != m_file);
	fclose(m_file);
	m_file = nullptr;
}

} } // namespace terark::terichdb
