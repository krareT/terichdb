#ifndef __terark_db_db_conf_hpp__
#define __terark_db_db_conf_hpp__

#include <terark/hash_strmap.hpp>
#include <terark/gold_hash_map.hpp>
#include <terark/bitmap.hpp>
#include <terark/pass_by_value.hpp>
#include <terark/util/refcount.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/type_traits/is_arithmetic.hpp>
#include <boost/version.hpp>
#include "db_dll_decl.hpp"

#if BOOST_VERSION < 106000
	#error boost version must >= 1.6
#endif

#define TERARK_DB_NON_COPYABLE_CLASS(Class) \
	Class(const Class&) = delete; \
	Class(Class&&) = delete; \
	Class& operator=(const Class&) = delete; \
	Class& operator=(Class&&) = delete

namespace terark { namespace db {

	struct ClassMember_name {
		template<class X, class Y>
		bool operator()(const X& x, const Y& y) const { return x < y; }
		template<class T>
		const std::string& operator()(const T& x) const { return x.name; }
	};

	enum class ColumnType : unsigned char {
		// all number types are LittleEndian
		Any, // real type is stored as first byte of the data
		Nested,
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
		Decimal128,
		Uuid,    // 16 bytes(128 bits) binary
		Fixed,   // Fixed length binary
		VarSint,
		VarUint,
		StrZero, // Zero ended string
		TwoStrZero, // Special, now just for BSON RegEx type
		Binary,  // Prefixed by length(var_uint) in bytes
		CarBin,  // Cardinal Binary, prefixed by uint32 length
	};

	struct TERARK_DB_DLL ColumnMeta {
		uint32_t fixedLen;
		uint32_t fixedOffset;
		uint32_t reserved0;
	//	static_bitmap<16, uint16_t> flags;
		unsigned char reserved1;
		unsigned char mysqlType;
		ColumnType type;
		unsigned char mongoType; // user column type, such as mongodb type
	//	ColumnMeta();
		bool isInteger() const;
		bool isNumber() const;
		bool isString() const;
		explicit ColumnMeta(ColumnType);
		size_t fixedEndOffset() const { return fixedOffset + fixedLen; }

		static ColumnType numberType(int       x) { return sintType(x); }
		static ColumnType numberType(short     x) { return sintType(x); }
		static ColumnType numberType(long      x) { return sintType(x); }
		static ColumnType numberType(long long x) { return sintType(x); }

		static ColumnType numberType(unsigned int       x) { return uintType(x); }
		static ColumnType numberType(unsigned short     x) { return uintType(x); }
		static ColumnType numberType(unsigned long      x) { return uintType(x); }
		static ColumnType numberType(unsigned long long x) { return uintType(x); }

		static ColumnType numberType(float  x) { return ColumnType::Float32; }
		static ColumnType numberType(double x) { return ColumnType::Float64; }
		static ColumnType numberType(long double x) {
			if (sizeof(long double) > 8) // 16 for gcc
				return ColumnType::Float128;
			else
				return ColumnType::Float64; // msvc
		}

	private:
		template<class Uint>
		static ColumnType uintType(Uint) {
			if (sizeof(Uint) ==  1) return ColumnType::Uint08;
			if (sizeof(Uint) ==  2) return ColumnType::Uint16;
			if (sizeof(Uint) ==  4) return ColumnType::Uint32;
			if (sizeof(Uint) ==  8) return ColumnType::Uint64;
			if (sizeof(Uint) == 16) return ColumnType::Uint128;
			abort();
			return ColumnType::Any;
		}
		template<class Sint>
		static ColumnType sintType(Sint) {
			if (sizeof(Sint) ==  1) return ColumnType::Sint08;
			if (sizeof(Sint) ==  2) return ColumnType::Sint16;
			if (sizeof(Sint) ==  4) return ColumnType::Sint32;
			if (sizeof(Sint) ==  8) return ColumnType::Sint64;
			if (sizeof(Sint) == 16) return ColumnType::Sint128;
			abort();
			return ColumnType::Any;
		}
	};

	class TERARK_DB_DLL ColumnVec {
	public:
		struct Elem {
			uint32_t pos;
			uint32_t len;
			Elem() : pos(UINT32_MAX), len(UINT32_MAX) {}
			Elem(uint32_t p, uint32_t n) : pos(p), len(n) {}
			bool isValid() const { return UINT32_MAX != pos; }
		};
		const  byte*  m_base;
		valvec<Elem>  m_cols;

		~ColumnVec();
		ColumnVec();
		ColumnVec(size_t cap, valvec_reserve);
		ColumnVec(const ColumnVec&);
		ColumnVec(ColumnVec&&);
		ColumnVec& operator=(const ColumnVec&);
		ColumnVec& operator=(ColumnVec&&);

		bool empty() const { return m_cols.empty(); }
		void erase_all() {
			m_base = nullptr;
			m_cols.erase_all();
		}
		size_t size() const { return m_cols.size(); }
		fstring operator[](size_t idx) const {
			assert(idx < m_cols.size());
			Elem e = m_cols[idx];
			return fstring(m_base + e.pos, e.len);
		}
		Elem get_elem(size_t idx) const {
			assert(idx < m_cols.size());
			return m_cols[idx];
		}
		void grow(size_t inc) { m_cols.grow(inc); }
		void push_back(size_t pos, size_t len) {
			m_cols.push_back({uint32_t(pos), uint32_t(len)});
		}
		void push_back(Elem e) { m_cols.push_back(e); }
		void reserve(size_t cap) { m_cols.reserve(cap); }

		template<class NumberType>
		typename boost::enable_if<boost::is_arithmetic<NumberType>, NumberType>::type
		getNumber(size_t idx) const {
			assert(idx < m_cols.size());
			Elem e = m_cols[idx];
			assert(sizeof(NumberType) == e.len);
			return unaligned_load<NumberType>(m_base + e.pos);
		}
	};

	class TERARK_DB_DLL Schema : public RefCounter {
		friend class SchemaSet;
	public:
		static const size_t MaxProjColumns = 64;
		Schema();
		~Schema();
		void compile(const Schema* parent = nullptr);

		void parseRow(fstring row, ColumnVec* columns) const;
		void parseRowAppend(fstring row, size_t start, ColumnVec* columns) const;
		void combineRow(const ColumnVec& myCols, valvec<byte>* myRowData) const;
		void combineRowAppend(const ColumnVec& myCols, valvec<byte>* myRowData) const;

		void projectToNorm(fstring col, size_t columnId, valvec<byte>* rowData) const;
		void projectToLast(fstring col, size_t columnId, valvec<byte>* rowData) const;

		void selectParent(const ColumnVec& parentCols, valvec<byte>* myRowData) const;
		void selectParent(const ColumnVec& parentCols, ColumnVec* myCols) const;

		size_t parentColumnId(size_t myColumnId) const {
			assert(m_proj.size() == m_columnsMeta.end_i());
			assert(myColumnId < m_proj.size());
			return m_proj[myColumnId];
		}
		const valvec<size_t>& getProj() const { return m_proj; }

		void byteLexEncode(valvec<byte>&) const;
		void byteLexDecode(valvec<byte>&) const;
		void byteLexEncode(byte* data, size_t size) const;
		void byteLexDecode(byte* data, size_t size) const;

		size_t parseDelimText(char delim, fstring text, valvec<byte>* row) const;

		std::string toJsonStr(fstring row) const;
		std::string toJsonStr(const char* row, size_t rowlen) const;

		ColumnType getColumnType(size_t columnId) const;
		fstring getColumnName(size_t columnId) const;
		size_t getColumnId(fstring columnName) const;
		const ColumnMeta& getColumnMeta(size_t columnId) const;
		size_t columnNum() const { return m_columnsMeta.end_i(); }

		size_t getFixedRowLen() const { return m_fixedLen; }

		bool should_use_FixedLenStore() const;

		static ColumnType parseColumnType(fstring str);
		static const char* columnTypeStr(ColumnType);

		std::string joinColumnNames(char delim = ',') const;

		int compareData(fstring x, fstring y) const;

		// used by glibc.qsort_r or msvc.qsort_s
		struct CompareByIndexContext {
			const Schema* schema;
			const char*   basePtr;
			const uint32_t* offsets;
		};
		static int QsortCompareFixedLen(const void* x, const void* y, const void* ctx);
		static int QsortCompareByIndex(const void* x, const void* y, const void* ctx);

		hash_strmap<ColumnMeta> m_columnsMeta;
		std::string m_name;
		std::string m_nltDelims;

		// if not zero, len of (m_lastVarLenCol-1) is omitted
		size_t m_lastVarLenCol;
		size_t m_restFixLenSum; // len sum of [m_lastVarLenCol, colnum)
		int    m_minFragLen;
		int    m_maxFragLen;
		int    m_sufarrMinFreq;
		int    m_rankSelectClass;
		float  m_dictZipSampleRatio;
		byte   m_nltNestLevel;

		bool   m_isCompiled: 1;
		bool   m_isOrdered : 1; // just for index schema
//		bool   m_isPrimary : 1;
		bool   m_isUnique  : 1;
		bool   m_needEncodeToLexByteComparable : 1;
		bool   m_canEncodeToLexByteComparable  : 1;
		bool   m_useFastZip : 1;
		bool   m_dictZipLocalMatch : 1;
		bool   m_isInplaceUpdatable: 1;
		bool   m_enableLinearScan  : 1;
		bool   m_mmapPopulate : 1;
		static_bitmap<MaxProjColumns> m_keepCols;

		// used for ordered index, m_indexOrder.is1(i) means i'th column
		// is ordered desc, don't support in first version
		// static_bitmap<MaxProjColumns> m_indexOrder;

	protected:
		void compileProject(const Schema* parent);
		size_t computeFixedRowLen() const; // return 0 if RowLen is not fixed
		template<class Converter>
		void byteLexConvert(byte* data, size_t size) const;

	protected:
		size_t m_fixedLen;
	/*
	// Backlog: select from multiple tables
		struct ColumnLink {
			const Schema* parent;
			size_t        proj; // column[i] is from parent->column[proj[i]]
		};
		valvec<ColumnLink> m_columnsLink;
	*/
		const Schema*    m_parent;
		valvec<size_t>   m_proj;

	public:
		// Helpers for define & serializing object
		template<int N>
		struct Fixed {
			char data[N];
			Fixed() { memset(data, 0, N); }
			operator fstring() const { return fstring(data, N); }
			template<class DIO> friend void
			DataIO_loadObject(DIO& dio, Fixed& x) { dio.ensureRead(&x, N); }
			template<class DIO> friend void
			DataIO_saveObject(DIO& dio,const Fixed&x){dio.ensureWrite(&x,N);}
			template<class DIO>
			friend boost::mpl::true_
			Deduce_DataIO_is_dump(DIO*,Fixed*){return boost::mpl::true_();}
			friend boost::mpl::false_
			Deduce_DataIO_need_bswap(Fixed*){return boost::mpl::false_();}
		};
		template<class Str>
		class StrZeroLoader {
			Str* str;
		public:
			StrZeroLoader(Str& s) : str(&s) {}
			template<class DataIO>
			friend void DataIO_loadObject(DataIO& dio, StrZeroLoader& x) {
				unsigned char c;
				do { dio >> c;
					 x.str->push_back(c);
				} while (0 != c);
			}
		};
		template<class Str>
		class StrZeroSaver : public fstring {
		public:
			StrZeroSaver(const Str& x) : fstring(x.data(), x.size()) {}
			template<class DataIO>
			friend void DataIO_saveObject(DataIO&dio,const StrZeroSaver&x){
				if (x.size() == 0)
					dio << (unsigned char)(0);
				else {
					dio.ensureWrite(x.data(), x.size());
					const size_t len = strnlen(x.data(), x.size());
					assert(len==x.size() || len+1==x.size());
					if ( !(len==x.size() || len+1==x.size()) ) {
						THROW_STD(invalid_argument,
							"strZero.size=%zd strnlen=%zd", x.size(), len);
					}
					if (x.size() == len)
						dio << (unsigned char)(0);
				}
			}
		};
		// StrZero will not be serialized as Last Column
		template<class Str>
		static pass_by_value<StrZeroLoader<Str> >
		StrZero(Str& x) { return StrZeroLoader<Str>(x); }
		template<class Str>
		static StrZeroSaver<Str>
		StrZero(const Str& x) { return StrZeroSaver<Str>(x); }

		class CarBin : public valvec<byte_t> {
			template<class DataIO>
			friend void DataIO_loadObject(DataIO& dio, CarBin& x) {
				uint32_t len;
				dio >> len;
				x.resize_no_init(len);
				dio.ensureRead(x.begin(), len);
			}
			template<class DataIO>
			friend void DataIO_saveObject(DataIO& dio, const CarBin& x) {
				dio << uint32_t(x.size());
				dio.ensureWrite(x.begin(), x.size());
			}
		public:
			using valvec<byte_t>::valvec;
		};

		class TwoStrZero : public std::pair<std::string, std::string> {
			template<class DataIO>
			friend void DataIO_loadObject(DataIO& dio, TwoStrZero& x) {
				dio & StrZero(x.first) & StrZero(x.second);
			}
			template<class DataIO>
			friend void DataIO_saveObject(DataIO& dio, const TwoStrZero& x) {
				dio & StrZero(x.first) & StrZero(x.second);
			}
		public:
			using std::pair<std::string, std::string>::pair;
		};

		template<class NumberType>
		static inline
		typename boost::enable_if<boost::is_arithmetic<NumberType>, NumberType>::type
		numberOf(fstring coldata) {
			assert(coldata.size() == sizeof(NumberType));
			return unaligned_load<NumberType>(coldata.p);
		}

		template<class NumberType>
		static inline
		typename boost::enable_if<boost::is_arithmetic<NumberType>, fstring>::type
		fstringOf(const NumberType* x) {
			return fstring((const char*)x, sizeof(NumberType));
		}

		template<class NumberType>
		inline
		typename boost::enable_if<boost::is_arithmetic<NumberType>, NumberType>::type
		getNumber(const ColumnVec& colvec, size_t columnId) const {
		#if !defined(NDEBUG)
			const ColumnMeta& colmeta = m_columnsMeta.val(columnId);
			assert(columnId < colvec.size());
			assert(colvec.size() == m_columnsMeta.end_i());
			assert(colmeta.isNumber());
			assert(colmeta.fixedLen == sizeof(NumberType));
			assert(colmeta.numberType(NumberType()) == colmeta.type);
		#endif
			ColumnVec::Elem e = colvec.m_cols[columnId];
			assert(sizeof(NumberType) == e.len);
			return unaligned_load<NumberType>(colvec.m_base + e.pos);
		}
	};
	typedef boost::intrusive_ptr<Schema> SchemaPtr;

	// a set of schema, could be all indices of a table
	// or all column groups of a table
	class TERARK_DB_DLL SchemaSet : public RefCounter {
		struct TERARK_DB_DLL Hash {
			size_t operator()(const SchemaPtr& x) const;
			size_t operator()(fstring x) const;
		};
		struct TERARK_DB_DLL Equal {
			bool operator()(const SchemaPtr& x, const SchemaPtr& y) const;
			bool operator()(const SchemaPtr& x, fstring y) const;
			bool operator()(fstring x, const SchemaPtr& y) const
			  { return (*this)(y, x); }
		};
		struct MyKeyExtractor {
			const std::string&
			operator()(const SchemaPtr& x) const { return x->m_name; }
		};
		typedef hash_and_equal< fstring
							  , fstring_func::hash_align
							  , fstring_func::equal_align
							  > MyHashEqual;
	public:
		SchemaSet();
		~SchemaSet();
		SchemaPtr m_uniqIndexFields;
	//	gold_hash_set<SchemaPtr, Hash, Equal> m_nested;
		gold_hash_tab<std::string, SchemaPtr, MyHashEqual, MyKeyExtractor> m_nested;
		size_t m_flattenColumnNum;
		size_t indexNum() const { return m_nested.end_i(); }
		const Schema* getSchema(size_t nth) const {
			assert(nth < m_nested.end_i());
			return &*m_nested.elem_at(nth);
		}
		void compileSchemaSet(const Schema* parent);
	};
	typedef boost::intrusive_ptr<SchemaSet> SchemaSetPtr;

	class TERARK_DB_DLL SchemaConfig : public RefCounter {
	public:
		struct Colproject {
			uint32_t colgroupId;
			uint32_t subColumnId;
		};
		SchemaPtr      m_rowSchema;
		SchemaPtr      m_wrtSchema;
		SchemaSetPtr   m_indexSchemaSet;
		SchemaSetPtr   m_colgroupSchemaSet;
		valvec<size_t> m_uniqIndices;
		valvec<size_t> m_multIndices;
		valvec<size_t> m_updatableColgroups; // index of m_colgroupSchemaSet
		valvec<size_t> m_rowSchemaColToWrtCol;
		valvec<Colproject> m_colproject; // parallel with m_rowSchema
		llong    m_compressingWorkMemSize;
		llong    m_maxWritingSegmentSize;
		size_t   m_minMergeSegNum;
		double   m_purgeDeleteThreshold;
		std::string m_tableClass;
		bool     m_usePermanentRecordId;

		SchemaConfig();
		~SchemaConfig();

		const Schema& getIndexSchema(size_t indexId) const {
			assert(indexId < getIndexNum());
			return *m_indexSchemaSet->m_nested.elem_at(indexId);
		}
		const SchemaSet& getIndexSchemaSet() const { return *m_indexSchemaSet; }
		size_t getIndexNum() const { return m_indexSchemaSet->m_nested.end_i(); }
		size_t getIndexId(fstring indexColumnNames) const {
			return m_indexSchemaSet->m_nested.find_i(indexColumnNames);
		}

		const Schema& getColgroupSchema(size_t colgroupId) const {
			assert(colgroupId < getColgroupNum());
			return *m_colgroupSchemaSet->m_nested.elem_at(colgroupId);
		}
		const SchemaSet& getColgroupSchemaSet() const { return *m_colgroupSchemaSet; }
		size_t getColgroupNum() const { return m_colgroupSchemaSet->m_nested.end_i(); }
		size_t getColgroupId(fstring colgroupColumnNames) const {
			return m_colgroupSchemaSet->m_nested.find_i(colgroupColumnNames);
		}

		const Schema& getRowSchema() const { return *m_rowSchema; }
		size_t columnNum() const { return m_rowSchema->columnNum(); }

		bool isInplaceUpdatableColumn(size_t columnId) const;
		bool isInplaceUpdatableColumn(fstring colname) const;

		void loadJsonString(fstring jstr);
		void loadJsonFile(fstring fname);
		void saveJsonFile(fstring fname) const;

		void loadMetaDFA(fstring fname);
		void saveMetaDFA(fstring fname) const;

	protected:
		void compileSchema();
	};
	typedef boost::intrusive_ptr<SchemaConfig> SchemaConfigPtr;

	struct TERARK_DB_DLL DbConf {
		std::string dir;
	};

} } // namespace terark::db

#endif //__terark_db_db_conf_hpp__
