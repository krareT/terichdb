#ifndef __nark_db_db_conf_hpp__
#define __nark_db_db_conf_hpp__

#include <string>
#include <nark/hash_strmap.hpp>
#include <nark/gold_hash_map.hpp>
#include <nark/bitmap.hpp>
#include <nark/io/StreamBuffer.hpp>
#include <nark/util/fstrvec.hpp>
#include <nark/util/refcount.hpp>
#include <boost/intrusive_ptr.hpp>

namespace nark {

	struct ClassMember_name {
		template<class X, class Y>
		bool operator()(const X& x, const Y& y) const { return x < y; }
		template<class T>
		const std::string& operator()(const T& x) const { return x.name; }
	};

	enum class SortOrder : unsigned char {
		Ascending,
		Descending,
		UnOrdered,
	};

	enum class ColumnType : unsigned char {
		// all number types are LittleEndian
		WholeRow,
		Uint08,
		Sint08,
		Uint16,
		Sint16,
		Uint32,
		Sint32,
		Uint64,
		Sint64,
		Uint128,
		Sint128,
		Float32,
		Float64,
		Float128,
		Uuid,    // 16 bytes(128 bits) binary
		Fixed,   // Fixed length binary
		StrZero, // Zero ended string
		StrUtf8, // Prefixed by length(var_uint) in bytes
		Binary,  // Prefixed by length(var_uint) in bytes
	};

	struct ColumnMeta {
		uint32_t fixedLen = 0;
		static_bitmap<16, uint16_t> flags;
		ColumnType type = ColumnType::WholeRow;
	};
	struct ColumnData : fstring {
		ColumnType type;
		unsigned char preLen = 0;
		unsigned char postLen = 0;
		size_t all_size() const { return fstring::n + preLen + postLen; }
		const char* all_data() const { return fstring::p - preLen; }
		ColumnData() : type(ColumnType::WholeRow) {}
		explicit ColumnData(ColumnType t) : type(t) {}
		ColumnData(const ColumnMeta& meta, fstring row);
		const fstring& fstr() const { return *this; }
	};
	class Schema : virtual public RefCounter {
	public:
		void parseRow(fstring row, valvec<ColumnData>* columns) const;
		void parseRowAppend(fstring row, valvec<ColumnData>* columns) const;

		ColumnType getColumnType(size_t columnId) const;
		fstring getColumnName(size_t columnId) const;
		size_t getColumnId(fstring columnName) const;
		const ColumnMeta& getColumnMeta(size_t columnId) const;
		size_t columnNum() const { return m_columnsMeta.end_i(); }

		size_t getFixedRowLen() const; // return 0 if RowLen is not fixed

		static ColumnType parseColumnType(fstring str);

		hash_strmap<ColumnMeta> m_columnsMeta;
	};
	typedef boost::intrusive_ptr<Schema> SchemaPtr;

	// a set of schema, could be all indices of a table
	// or all column groups of a table
	class SchemaSet : virtual public RefCounter {
		struct Hash {
			size_t operator()(const SchemaPtr& x) const;
			size_t operator()(fstring x) const;
		};
		struct Equal {
			bool operator()(const SchemaPtr& x, const SchemaPtr& y) const;
			bool operator()(const SchemaPtr& x, fstring y) const;
			bool operator()(fstring x, const SchemaPtr& y) const
			  { return (*this)(y, x); }
		};
	public:
		gold_hash_set<SchemaPtr, Hash, Equal> m_nested;
		febitvec m_keepColumn;
		febitvec m_keepSchema;
		void compileSchemaSet();
		void parseNested(const valvec<fstring>& nested, valvec<ColumnData>* flatten) const;
	};
	typedef boost::intrusive_ptr<SchemaSet> SchemaSetPtr;

	struct DbConf {
		std::string dir;
	};

	class BaseContext : public RefCounter {
	public:
	};
	typedef boost::intrusive_ptr<BaseContext> BaseContextPtr;

} // namespace

#endif //__nark_db_db_conf_hpp__
