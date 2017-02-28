
#pragma once

#include <terark/terichdb/db_table.hpp>
#include <terark/terichdb/db_segment.hpp>

namespace terark { namespace terichdb { namespace trbdb {

class TERICHDB_DLL TrbWritableIndex : public ReadableIndex, public WritableIndex, public ReadableStore, public WritableStore {
public:
    static TrbWritableIndex *createIndex(Schema const &);

    virtual bool removeWithSeqId(fstring key, llong id, uint64_t &seq, DbContext*) = 0;
    virtual bool insertWithSeqId(fstring key, llong id, uint64_t &seq, DbContext*) = 0;
    virtual uint64_t allocSeqId() = 0;
};
typedef boost::intrusive_ptr<TrbWritableIndex> TrbWritableIndexPtr;

}}} // namespace terark::terichdb::wt

