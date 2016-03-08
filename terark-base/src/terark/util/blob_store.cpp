#include "blob_store.hpp"

namespace terark {

BlobStore::BlobStore() {
	m_numRecords = 0;
}
BlobStore::~BlobStore() {
}

void BlobStore::risk_swap(BlobStore& y) {
	std::swap(m_numRecords, y.m_numRecords);
}

void BlobStore::get_record(size_t recID, valvec<byte_t>* recData) const {
	recData->erase_all();
	get_record_append(recID, recData);
}

valvec<byte_t> BlobStore::get_record(size_t recID) const {
	valvec<byte_t> data;
	get_record(recID, &data);
	return data;
}

} // namespace terark

