#ifndef __nark_db_db_conf_hpp__
#define __nark_db_db_conf_hpp__

#include <nark/hash_strmap.hpp>
#include <nark/gold_hash_map.hpp>
#include <nark/bitmap.hpp>
#include <nark/pass_by_value.hpp>
#include <nark/util/refcount.hpp>
#include <boost/intrusive_ptr.hpp>

#if defined(_MSC_VER)

#  if defined(NARK_DB_CREATE_DLL)
#    pragma warning(disable: 4251)
#    define NARK_DB_DLL __declspec(dllexport)      // creator of dll
#    if defined(_DEBUG) || !defined(NDEBUG)
#//	   pragma message("creating nark-db-d.lib")
#    else
#//	   pragma message("creating nark-db-r.lib")
#    endif
#  elif defined(NARK_DB_USE_DLL)
#    pragma warning(disable: 4251)
#    define NARK_DB_DLL __declspec(dllimport)      // user of dll
#    if defined(_DEBUG) || !defined(NDEBUG)
//#	   pragma comment(lib, "nark-db-d.lib")
#    else
//#	   pragma comment(lib, "nark-db-r.lib")
#    endif
#  else
#    define NARK_DB_DLL                            // static lib creator or user
#  endif

#else /* _MSC_VER */

#  define NARK_DB_DLL

#endif /* _MSC_VER */

#define NARK_DB_NON_COPYABLE_CLASS(Class) \
	Class(const Class&) = delete; \
	Class(Class&&) = delete; \
	Class& operator=(const Class&) = delete; \
	Class& operator=(Class&&) = delete

namespace nark { namespace db {

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

	struct NARK_DB_DLL ColumnMeta {
		uint32_t fixedLen;
	//	static_bitmap<16, uint16_t> flags;
		unsigned char reserved1;
		unsigned char reserved2;
		ColumnType type;
		unsigned char uType; // user column type, such as mongodb type
		ColumnMeta();
		bool isInteger() const;
		bool isNumber() const;
		explicit ColumnMeta(ColumnType);
	};

	class NARK_DB_DLL Schema : public RefCounter {
		friend class SchemaSet;
	public:
		static const size_t MaxProjColumns = 64;
		Schema();
		~Schema();
		void compile(const Schema* parent = nullptr);

		void parseRow(fstring row, valvec<fstring>* columns) const;
		void parseRowAppend(fstring row, valvec<fstring>* columns) const;
		void combineRow(const valvec<fstring>& myCols, valvec<byte>* myRowData) const;

		void projectToNorm(fstring col, size_t columnId, valvec<byte>* rowData) const;
		void projectToLast(fstring col, size_t columnId, valvec<byte>* rowData) const;

		void selectParent(const valvec<fstring>& parentCols, valvec<byte>* myRowData) const;
		void selectParent(const valvec<fstring>& parentCols, valvec<fstring>* myCols) const;

		size_t parentColumnId(size_t myColumnId) const {
			assert(m_proj.size() == m_columnsMeta.end_i());
			assert(myColumnId < m_proj.size());
			return m_proj[myColumnId];
		}

		void byteLexConvert(valvec<byte>&) const;
		void byteLexConvert(byte* data, size_t size) const;

		size_t parseDelimText(char delim, fstring text, valvec<byte>* row) const;

		std::string toJsonStr(fstring row) const;
		std::string toJsonStr(const char* row, size_t rowlen) const;

		ColumnType getColumnType(size_t columnId) const;
		fstring getColumnName(size_t columnId) const;
		size_t getColumnId(fstring columnName) const;
		const ColumnMeta& getColumnMeta(size_t columnId) const;
		size_t columnNum() const { return m_columnsMeta.end_i(); }

		size_t getFixedRowLen() const { return m_fixedLen; }

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

		bool   m_isOrdered : 1; // just for index schema
		bool   m_isUnique  : 1;
		bool   m_needEncodeToLexByteComparable : 1;
		bool   m_canEncodeToLexByteComparable  : 1;
		bool   m_useFastZip : 1;
		bool   m_dictZipLocalMatch : 1;
		static_bitmap<MaxProjColumns> m_keepCols;

		// used for ordered index, m_indexOrder.is1(i) means i'th column
		// is ordered desc, don't support in first version
		// static_bitmap<MaxProjColumns> m_indexOrder;

	protected:
		void compileProject(const Schema* parent);
		size_t computeFixedRowLen() const; // return 0 if RowLen is not fixed

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
		valvec<unsigned> m_proj;

	public:
		// Helpers for define & serializing object
		template<int N>
		struct Fixed {
			char data[N];
			Fixed() { memset(data, 0, N); }
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
	};
	typedef boost::intrusive_ptr<Schema> SchemaPtr;

	// a set of schema, could be all indices of a table
	// or all column groups of a table
	class NARK_DB_DLL SchemaSet : public RefCounter {
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
		SchemaSet();
		~SchemaSet();
		SchemaPtr m_uniqIndexFields;
		gold_hash_set<SchemaPtr, Hash, Equal> m_nested;
		size_t m_flattenColumnNum;
		size_t indexNum() const { return m_nested.end_i(); }
		const Schema* getSchema(size_t nth) const {
			assert(nth < m_nested.end_i());
			return &*m_nested.elem_at(nth);
		}
		void compileSchemaSet(const Schema* parent);
	};
	typedef boost::intrusive_ptr<SchemaSet> SchemaSetPtr;

	class NARK_DB_DLL SchemaConfig : public RefCounter {
	public:
		struct Colproject {
			uint32_t colgroupId;
			uint32_t subColumnId;
		};
		SchemaPtr     m_rowSchema;
		SchemaSetPtr  m_indexSchemaSet;
		SchemaSetPtr  m_colgroupSchemaSet;
		valvec<size_t> m_uniqIndices;
		valvec<size_t> m_multIndices;
		valvec<Colproject> m_colproject; // parallel with m_rowSchema
		llong m_readonlyDataMemSize;
		llong m_maxWrSegSize;

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

		void loadJsonString(fstring jstr);
		void loadJsonFile(fstring fname);
		void saveJsonFile(fstring fname) const;

		void loadMetaDFA(fstring fname);
		void saveMetaDFA(fstring fname) const;

	protected:
		void compileSchema();
	};
	typedef boost::intrusive_ptr<SchemaConfig> SchemaConfigPtr;

	struct NARK_DB_DLL DbConf {
		std::string dir;
	};

} } // namespace nark::db

#endif //__nark_db_db_conf_hpp__
