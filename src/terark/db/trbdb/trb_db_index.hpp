
#pragma once

#include <terark/db/db_table.hpp>
#include <terark/db/db_segment.hpp>

namespace terark { namespace db { namespace trb {

class TERARK_DB_DLL TrbWritableIndex : public ReadableIndex, public WritableIndex, public ReadableStore, public WritableStore {
public:
    static TrbWritableIndex *createIndex(Schema const &, PathRef);
};
typedef boost::intrusive_ptr<TrbWritableIndex> TrbWritableIndexPtr;

}}} // namespace terark::db::wt

