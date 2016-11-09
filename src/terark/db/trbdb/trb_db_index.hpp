
#pragma once

#include <terark/db/db_table.hpp>
#include <terark/db/db_segment.hpp>

namespace terark { namespace db { namespace trbdb {

class TERARK_DB_DLL TrbWritableIndex : public ReadableIndex, public WritableIndex, public ReadableStore, public WritableStore {
public:
    static TrbWritableIndex *createIndex(Schema const &);

    virtual bool removeWithSeqId(fstring key, llong id, uint64_t &seq, DbContext*) = 0;
    virtual bool insertWithSeqId(fstring key, llong id, uint64_t &seq, DbContext*) = 0;
};
typedef boost::intrusive_ptr<TrbWritableIndex> TrbWritableIndexPtr;

}}} // namespace terark::db::wt

