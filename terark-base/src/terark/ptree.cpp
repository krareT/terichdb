#include "ptree.hpp"
#include <stdio.h>
#include <terark/num_to_str.hpp>

#ifdef _MSC_VER
	#define snprintf _snprintf_s
#endif

namespace terark {
namespace objbox {

#define int_to_str num_to_str

void array_for_each(std::string& path, obj_array* arr, const function<void(fstring, obj&)>& op) {
	op(path, *arr);
	for(long i = 0, n = arr->size(); i < n; ++i) {
		long oldsize = path.size();
		{
			char name[24] = "/";
			path.append(name, int_to_str(name+1, i) + 1);
		}
		if (ptree* x = dynamic_cast<ptree*>((*arr)[i].get()))
			x->for_each(path, op);
		else if (obj_array* x = dynamic_cast<obj_array*>((*arr)[i].get()))
			array_for_each(path, x, op);
		else
			op(path, *(*arr)[i]);
		path.resize(oldsize);
	}
}
ptree::~ptree() {}

obj* ptree::get(fstring path) const {
	ptrdiff_t i = 0;
	ptree* x = const_cast<ptree*>(this);
	while (i < path.n) {
		ptrdiff_t j = i;
		while (j < path.n && '/' != path[j]) ++j;
		fstring name(path.p+i, j-i);
		size_t f = x->children.find_i(name);
		if (x->children.end_i() == f)
			return NULL;
		else {
			obj* y = x->children.val(f).get();
			if (path.n == j)
				return y;
			else {
				x = dynamic_cast<ptree*>(y);
				if (NULL == x)
					return NULL;
			}
		}
		i = j + 1;
	}
	return x;
}

fstring ptree::get(fstring path, fstring Default) const {
	obj* x = get(path);
	if (NULL == x)
		return Default;
	else {
		obj_string* y = dynamic_cast<obj_string*>(x);
		if (NULL == y)
			return Default;
		else
			return fstring(y->data(), y->size());
	}
}
#define TERARK_PTREE_METHOD_IMPL_get(Boxer, Boxed) \
Boxed ptree::get(fstring path, Boxed Default) const { \
	obj* x = get(path); \
	if (NULL == x) \
		return Default; \
	else { \
		Boxer* y = dynamic_cast<Boxer*>(x); \
		if (NULL == y) \
			return Default; \
		else \
			return (Boxed)(y->t); \
	} }
//--------------------------------------------------------------------
TERARK_PTREE_METHOD_IMPL_get(obj_llong  , long long)
TERARK_PTREE_METHOD_IMPL_get(obj_ldouble, long double)

///@param bool on_node(name, obj** value): if return true, go on, else break
obj_ptr*
ptree::set(fstring path, const function<bool(fstring, obj**)>& on_node) {
	ptrdiff_t i = 0;
	ptree* x = this;
	while (i < path.n) {
		ptrdiff_t j = i;
		while (j < path.n && '/' != path[j]) ++j;
		fstring name(path.p+i, j-i);
		size_t f = x->children.insert_i(name).first;
		obj_ptr& y = x->children.val(f);
		{
			obj* py = y.get();
			bool go_on = on_node(name, &py);
			if (y.get() != py)
				y.reset(py);
			if (!go_on)
				return NULL;
		}
		if (path.n == j)
			return &y;

		ptree* z;
		if (y) {
			z = dynamic_cast<ptree*>(y.get());
			if (NULL == z) {
				std::string msg;
				msg += "path="; msg.append(path.p, j);
				msg += " must be ptree, but it is: "; msg += typeid(*z).name();
				fprintf(stderr, "%s\n", msg.c_str());
				throw std::logic_error(msg.c_str());
			}
		} else {
			y.reset(z = new ptree);
		}
		x = z;
		i = j + 1;
	}
	assert(0); // will not go here
	return NULL;
}

static bool AlwaysTrue(fstring, obj**) { return true; }

obj_ptr* ptree::add(fstring path) {
	return set(path, function<bool(fstring, obj**)>(&AlwaysTrue));
}

template<class Obj, class Ref = Obj>
struct OnNode_create {
	fstring path;
	Ref     val;
	explicit OnNode_create(fstring path1, Ref val1)
		: path(path1)
		, val(val1)
	{}
	bool operator()(fstring name, obj** x) const {
		if (path.end() == name.end() && NULL == *x) {
			*x = new typename obj_val<Obj>::type(val);
		}
		return true;
	}
};

void ptree::set(fstring path, const std::string& val) {
	set(path, OnNode_create<std::string, const std::string&>(path, val));
}
void ptree::set(fstring path, const char* val) {
	set(path, OnNode_create<fstring, fstring>(path, val));
}
void ptree::set(fstring path, fstring val) {
	set(path, OnNode_create<fstring, fstring>(path, val));
}

#define TERARK_PTREE_METHOD_IMPL_set(Boxed) \
	void ptree::set(fstring path, Boxed val) { \
		set(path, OnNode_create<Boxed>(path, val)); \
	}
//----------------------------------------------------------

TERARK_PTREE_METHOD_IMPL_set(long long)
TERARK_PTREE_METHOD_IMPL_set(long double)

void ptree::for_each_loop(std::string& path, const function<void(fstring, obj&)>& op) {
	op(path, *this);
	for (size_t i = 0; i != children.end_i(); ++i) {
		if (children.is_deleted(i)) continue;
		fstring name = children.key(i);
		path.append("/");
		path.append(name.p, name.n);
		if (ptree* x = dynamic_cast<ptree*>(children.val(i).get()))
			x->for_each_loop(path, op);
		else if (obj_array* x = dynamic_cast<obj_array*>(children.val(i).get()))
			array_for_each(path, x, op);
		else
			op(path, *children.val(i));
		path.resize(path.size() - children.key(i).n);
	}
}

void ptree::for_each(fstring base_dir, const function<void(fstring, obj&)>& op) {
	std::string path(base_dir.p, base_dir.n);
	for_each_loop(path, op);
}

//###########################################################################
//###########################################################################
// Json Serialization
//###########################################################################



//###########################################################################
//###########################################################################
// PHP Serialization
//###########################################################################

php_object::php_object() {}
php_object::~php_object() {}

TERARK_BOXING_OBJECT_DERIVE_IMPL(php_array, php_array_base)

namespace { // anonymous namespace
	struct MetaInfo {
		typedef void (*stringize_ft)(const obj*, std::string* out);
		stringize_ft to_php;
		stringize_ft to_json;
		size_t (*hash)(const obj*);
		MetaInfo() {
			memset(this, 0, sizeof(*this));
		}
	};
	struct BinaryDispatchMeta {
		bool  (*equal)(const obj*, const obj*);
	//	const std::typeinfo* type1;
	//	const std::typeinfo* type2;
	};
	hash_strmap<MetaInfo> g_meta;
	hash_strmap<BinaryDispatchMeta> g_binary_dispatch;

#define GEN_INT_FUNC(Int) \
	void php_stringize_##Int(const obj* self, std::string* out) {  \
		const obj_##Int* x = static_cast<const obj_##Int*>(self); \
		char buf[32] = {'i', ':' }; \
		size_t len = int_to_str(buf + 2, x->t) + 2; \
		buf[len++] = ';'; \
		out->append(buf, len); \
	} \
	size_t hash__##Int(const obj* self) { \
		const obj_##Int* x = static_cast<const obj_##Int*>(self); \
		return size_t(x->t); \
	}
//--------------------------------------------------------------------
	GEN_INT_FUNC(short)
	GEN_INT_FUNC(int)
	GEN_INT_FUNC(long)
	GEN_INT_FUNC(llong)
	GEN_INT_FUNC(ushort)
	GEN_INT_FUNC(uint)
	GEN_INT_FUNC(ulong)
	GEN_INT_FUNC(ullong)
#define GEN_unsupported_hash(type) \
	size_t hash__##type(const obj* self) { \
		std::string msg("Unsupported array key type: "); \
		msg += typeid(*self).name();  \
		throw std::logic_error(msg); \
		return 0; \
	}
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	GEN_unsupported_hash(bool)
	GEN_unsupported_hash(double)
	GEN_unsupported_hash(ldouble)
	GEN_unsupported_hash(array)

	size_t hash__string(const obj* self) {
		const obj_string* x = static_cast<const obj_string*>(self);
		return fstring_func::hash()(*x);
	}

	void php_stringize_bool(const obj* self, std::string* out) {
		const obj_bool* x = static_cast<const obj_bool*>(self);
		char buf[8] = "b:";
		size_t len = int_to_str(buf + 2, x->t) + 2;
		buf[len++] = ';';
		out->append(buf, len);
	}
	void php_stringize_string(const obj* self, std::string* out) {
		const obj_string* x = static_cast<const obj_string*>(self);
		out->append("s:");
		char buf[16];
		out->append(buf, int_to_str(buf, x->size()));
		out->append(":\"");
		out->append(*x);
		out->append("\";");
	}
	void php_stringize_double(const obj* self, std::string* out) {
		const obj_double* x = static_cast<const obj_double*>(self);
		char buf[96];
		out->append(buf, sprintf(buf, "d:%f;", x->t));
	}
	void php_stringize_ldouble(const obj* self, std::string* out) {
		const obj_ldouble* x = static_cast<const obj_ldouble*>(self);
		char buf[96];
		out->append(buf, sprintf(buf, "d:%Lf;", x->t));
	}
	void php_stringize_array(const obj* self, std::string* out) {
		const obj_array* x = static_cast<const obj_array*>(self);
		out->append("a:");
		{
			char buf[16];
			out->append(buf, int_to_str(buf, x->size()));
			out->append(":{");
		}
		for (long i = 0, n = x->size(); i < n; ++i) {
			{
				char buf[24] = "i:";
				size_t len = int_to_str(buf+2, i) + 2;
				buf[len++] = ';';
				out->append(buf, len);
			}
			php_append((*x)[i].get(), out);
		}
		out->append("}");
	}
/*
	void php_stringize_ptree(const obj* self, std::string* out) {
	// array with string keys
		const ptree* x = static_cast<const ptree*>(self);
		out->append("a:");
		{
			char buf[16];
			out->append(buf, int_to_str(buf, x->children.size()));
			out->append(":{");
		}
		for (long i = 0, n = x->children.end_i(); i < n; ++i) {
			if (x->children.is_deleted(i)) continue;
			out->append("s:");
			{
				fstring key(x->children.key(i));
				char buf[16];
				out->append(buf, int_to_str(buf, key.size()));
				out->append(":\"");
				out->append(key.p, key.n);
				out->append("\";");
			}
			php_append(x->children.val(i).get(), out);
		}
		out->append("}");
	}
*/
	void php_stringize_php_object(const obj* self, std::string* out) {
		const php_object* x = static_cast<const php_object*>(self);
// O:strlen(object name):object name:object size:{s:strlen(field name):field name:field definition;(repeated per field)}
		out->append("O:");
		{
			char buf[16];
			out->append(buf, int_to_str(buf, x->cls_name.size()));
			out->append(":\"");
			out->append(x->cls_name);
			out->append("\":");
			out->append(buf, int_to_str(buf, x->fields.size()));
			out->append(":{");
		}
		for (long i = 0, n = x->fields.end_i(); i < n; ++i) {
			if (x->fields.is_deleted(i)) continue;
			out->append("s:");
			{
				fstring key(x->fields.key(i));
				char buf[16];
				out->append(buf, int_to_str(buf, key.size()));
				out->append(":\"");
				out->append(key.p, key.n);
				out->append("\";");
			}
			php_append(x->fields.val(i).get(), out);
		}
		out->append("}");
	}
	void php_stringize_php_array(const obj* self, std::string* out) {
		const php_array* x = static_cast<const php_array*>(self);
		out->append("a:");
		{
			char buf[16];
			out->append(buf, int_to_str(buf, x->size()));
			out->append(":{");
		}
#if 1
		const bool freelist_is_using = x->freelist_is_using();
		for (long i = 0, n = x->end_i(); i < n; ++i) {
			if (freelist_is_using && x->is_deleted(i)) continue;
			if (!x->key(i)) continue;
			php_append(x->key(i).get(), out);
			php_append(x->val(i).get(), out);
		}
#else
		for (php_array::map_t::const_iterator iter = x->begin(); iter != x->end(); ++iter) {
			php_append(iter->first .get(), out);
			php_append(iter->second.get(), out);
		}
#endif
		out->append("}");
	}

#define GEN_2INTS_COMPARE(Int1, Int2) \
	bool equal__##Int1##__##Int2(const obj* p, const obj* q) { \
		const obj_##Int1* x = static_cast<const obj_##Int1*>(p); \
		const obj_##Int2* y = static_cast<const obj_##Int2*>(q); \
		typedef boost::mpl::if_c<(sizeof(x->t) > sizeof(y->t)) \
			, obj_##Int1::boxed_t \
			, obj_##Int2::boxed_t \
			>::type Int; \
		return (Int)(x->t) == (Int)(y->t); }
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

GEN_2INTS_COMPARE(short, short)
GEN_2INTS_COMPARE(short, int)
GEN_2INTS_COMPARE(short, long)
GEN_2INTS_COMPARE(short, llong)

GEN_2INTS_COMPARE(short, ushort)
GEN_2INTS_COMPARE(short, uint)
GEN_2INTS_COMPARE(short, ulong)
GEN_2INTS_COMPARE(short, ullong)
//--------------------------------------------

GEN_2INTS_COMPARE(int, short)
GEN_2INTS_COMPARE(int, int)
GEN_2INTS_COMPARE(int, long)
GEN_2INTS_COMPARE(int, llong)

GEN_2INTS_COMPARE(int, ushort)
GEN_2INTS_COMPARE(int, uint)
GEN_2INTS_COMPARE(int, ulong)
GEN_2INTS_COMPARE(int, ullong)
//--------------------------------------------

GEN_2INTS_COMPARE(long, short)
GEN_2INTS_COMPARE(long, int)
GEN_2INTS_COMPARE(long, long)
GEN_2INTS_COMPARE(long, llong)

GEN_2INTS_COMPARE(long, ushort)
GEN_2INTS_COMPARE(long, uint)
GEN_2INTS_COMPARE(long, ulong)
GEN_2INTS_COMPARE(long, ullong)
//--------------------------------------------

GEN_2INTS_COMPARE(llong, short)
GEN_2INTS_COMPARE(llong, int)
GEN_2INTS_COMPARE(llong, long)
GEN_2INTS_COMPARE(llong, llong)

GEN_2INTS_COMPARE(llong, ushort)
GEN_2INTS_COMPARE(llong, uint)
GEN_2INTS_COMPARE(llong, ulong)
GEN_2INTS_COMPARE(llong, ullong)
//--------------------------------------------

GEN_2INTS_COMPARE(ushort, short)
GEN_2INTS_COMPARE(ushort, int)
GEN_2INTS_COMPARE(ushort, long)
GEN_2INTS_COMPARE(ushort, llong)

GEN_2INTS_COMPARE(ushort, ushort)
GEN_2INTS_COMPARE(ushort, uint)
GEN_2INTS_COMPARE(ushort, ulong)
GEN_2INTS_COMPARE(ushort, ullong)
//--------------------------------------------

GEN_2INTS_COMPARE(uint, short)
GEN_2INTS_COMPARE(uint, int)
GEN_2INTS_COMPARE(uint, long)
GEN_2INTS_COMPARE(uint, llong)

GEN_2INTS_COMPARE(uint, ushort)
GEN_2INTS_COMPARE(uint, uint)
GEN_2INTS_COMPARE(uint, ulong)
GEN_2INTS_COMPARE(uint, ullong)
//--------------------------------------------

GEN_2INTS_COMPARE(ulong, short)
GEN_2INTS_COMPARE(ulong, int)
GEN_2INTS_COMPARE(ulong, long)
GEN_2INTS_COMPARE(ulong, llong)

GEN_2INTS_COMPARE(ulong, ushort)
GEN_2INTS_COMPARE(ulong, uint)
GEN_2INTS_COMPARE(ulong, ulong)
GEN_2INTS_COMPARE(ulong, ullong)
//--------------------------------------------

GEN_2INTS_COMPARE(ullong, short)
GEN_2INTS_COMPARE(ullong, int)
GEN_2INTS_COMPARE(ullong, long)
GEN_2INTS_COMPARE(ullong, llong)

GEN_2INTS_COMPARE(ullong, ushort)
GEN_2INTS_COMPARE(ullong, uint)
GEN_2INTS_COMPARE(ullong, ulong)
GEN_2INTS_COMPARE(ullong, ullong)
//--------------------------------------------

	size_t make_bin_name(char* buf, size_t buf_len, const std::type_info& t1, const std::type_info& t2) {
		const char* name1 = t1.name();
		const char* name2 = t2.name();
		size_t len1 = strlen(name1);
		size_t len2 = strlen(name2);
		if (len1 + len2 + 1 > buf_len) {
			std::string msg = "make_bin_name: typename too long: t1=";
			msg += t1.name();
			msg += ", t2=";
			msg += t2.name();
			throw std::runtime_error(msg);
		}
		buf[len1] = '+';
		memcpy(buf, name1, len1);
		memcpy(buf + len1 + 1, name2, len2);
		return len1+1+len2;
	}

	struct MetaInitializerClass {
		MetaInitializerClass() {
		#define ADD_META(type_sig) \
			do { \
				MetaInfo& mi = g_meta[typeid(obj_##type_sig).name()]; \
				mi.to_php = php_stringize_##type_sig; \
				mi.to_json = NULL; \
				mi.hash = hash__##type_sig; \
			} while (0)
			ADD_META(bool);
			ADD_META(short);
			ADD_META(int);
			ADD_META(long);
			ADD_META(llong);
			ADD_META(ushort);
			ADD_META(uint);
			ADD_META(ulong);
			ADD_META(ullong);
			ADD_META(double);
			ADD_META(ldouble);
			ADD_META(string);
			ADD_META(array);
			g_meta[typeid(php_array ).name()].to_php = php_stringize_php_array;
			g_meta[typeid(php_object).name()].to_php = php_stringize_php_object;

		#define ADD_BINARY_DISPATCH(type1, type2) \
			do { \
				BinaryDispatchMeta& bd = add_binary(typeid(obj_##type1), typeid(obj_##type2)); \
				bd.equal = equal__##type1##__##type2; \
			} while (0)
		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
			ADD_BINARY_DISPATCH(short, short);
			ADD_BINARY_DISPATCH(short, int);
			ADD_BINARY_DISPATCH(short, long);
			ADD_BINARY_DISPATCH(short, llong);

			ADD_BINARY_DISPATCH(short, ushort);
			ADD_BINARY_DISPATCH(short, uint);
			ADD_BINARY_DISPATCH(short, ulong);
			ADD_BINARY_DISPATCH(short, ullong);
			//--------------------------------------------

			ADD_BINARY_DISPATCH(int, short);
			ADD_BINARY_DISPATCH(int, int);
			ADD_BINARY_DISPATCH(int, long);
			ADD_BINARY_DISPATCH(int, llong);

			ADD_BINARY_DISPATCH(int, ushort);
			ADD_BINARY_DISPATCH(int, uint);
			ADD_BINARY_DISPATCH(int, ulong);
			ADD_BINARY_DISPATCH(int, ullong);
			//--------------------------------------------

			ADD_BINARY_DISPATCH(long, short);
			ADD_BINARY_DISPATCH(long, int);
			ADD_BINARY_DISPATCH(long, long);
			ADD_BINARY_DISPATCH(long, llong);

			ADD_BINARY_DISPATCH(long, ushort);
			ADD_BINARY_DISPATCH(long, uint);
			ADD_BINARY_DISPATCH(long, ulong);
			ADD_BINARY_DISPATCH(long, ullong);
			//--------------------------------------------

			ADD_BINARY_DISPATCH(llong, short);
			ADD_BINARY_DISPATCH(llong, int);
			ADD_BINARY_DISPATCH(llong, long);
			ADD_BINARY_DISPATCH(llong, llong);

			ADD_BINARY_DISPATCH(llong, ushort);
			ADD_BINARY_DISPATCH(llong, uint);
			ADD_BINARY_DISPATCH(llong, ulong);
			ADD_BINARY_DISPATCH(llong, ullong);
			//--------------------------------------------

			ADD_BINARY_DISPATCH(ushort, short);
			ADD_BINARY_DISPATCH(ushort, int);
			ADD_BINARY_DISPATCH(ushort, long);
			ADD_BINARY_DISPATCH(ushort, llong);

			ADD_BINARY_DISPATCH(ushort, ushort);
			ADD_BINARY_DISPATCH(ushort, uint);
			ADD_BINARY_DISPATCH(ushort, ulong);
			ADD_BINARY_DISPATCH(ushort, ullong);
			//--------------------------------------------

			ADD_BINARY_DISPATCH(uint, short);
			ADD_BINARY_DISPATCH(uint, int);
			ADD_BINARY_DISPATCH(uint, long);
			ADD_BINARY_DISPATCH(uint, llong);

			ADD_BINARY_DISPATCH(uint, ushort);
			ADD_BINARY_DISPATCH(uint, uint);
			ADD_BINARY_DISPATCH(uint, ulong);
			ADD_BINARY_DISPATCH(uint, ullong);
			//--------------------------------------------

			ADD_BINARY_DISPATCH(ulong, short);
			ADD_BINARY_DISPATCH(ulong, int);
			ADD_BINARY_DISPATCH(ulong, long);
			ADD_BINARY_DISPATCH(ulong, llong);

			ADD_BINARY_DISPATCH(ulong, ushort);
			ADD_BINARY_DISPATCH(ulong, uint);
			ADD_BINARY_DISPATCH(ulong, ulong);
			ADD_BINARY_DISPATCH(ulong, ullong);
			//--------------------------------------------

			ADD_BINARY_DISPATCH(ullong, short);
			ADD_BINARY_DISPATCH(ullong, int);
			ADD_BINARY_DISPATCH(ullong, long);
			ADD_BINARY_DISPATCH(ullong, llong);

			ADD_BINARY_DISPATCH(ullong, ushort);
			ADD_BINARY_DISPATCH(ullong, uint);
			ADD_BINARY_DISPATCH(ullong, ulong);
			ADD_BINARY_DISPATCH(ullong, ullong);
			//--------------------------------------------
		}

		BinaryDispatchMeta& add_binary(const std::type_info& t1, const std::type_info& t2) {
			char bin_name[1024];
			size_t len = make_bin_name(bin_name, sizeof(bin_name), t1, t2);
			return g_binary_dispatch[fstring(bin_name, len)];
		}
	};
	MetaInitializerClass g_MetaInitializer;
}

void php_save(const obj* self, std::string* out) {
	out->resize(0);
	php_append(self, out);
}

void php_append(const obj* self, std::string* out) {
	if (NULL == self) {
		out->append("N;");
	}
	else {
		size_t f = g_meta.find_i(typeid(*self).name());
		if (g_meta.end_i() != f) {
			g_meta.val(f).to_php(self, out);
		} else {
			std::string msg = "unknow type: ";
			msg += typeid(*self).name();
			throw std::runtime_error(msg);
		}
	}
}

obj* php_load_impl(const char* base, const char*& beg, const char* end) {
#define THROW_INPUT_ERROR2(msg, ptr) \
	do{ char buf[256]; \
		int blen = snprintf(buf, sizeof(buf), "%s at pos: %ld", msg, long(ptr-base)); \
		std::string strMsg(buf, blen); \
		throw std::logic_error(strMsg); \
	} while (0)
#define THROW_INPUT_ERROR(msg) THROW_INPUT_ERROR2(msg, beg)
	switch (*beg) {
		default: {
			std::string msg = "php_load: invalid type: ";
			msg.push_back(*beg);
			msg += ", input:";
			msg.append(beg, std::min<size_t>(100, end-beg));
			throw std::logic_error(msg);
		}
		case 'i': { // i:val;
			if (++beg >= end)
				THROW_INPUT_ERROR("php_load: Incompleted php integer");
			if (':' != *beg)
				THROW_INPUT_ERROR("php_load: integer, expect ':'");
			char* pp = NULL;
			long long val = strtoll(beg+1, &pp, 10);
			if (';' != *pp)
				THROW_INPUT_ERROR2("php_load: integer, expect ';'", pp);
			beg = pp + 1;
			if (sizeof(long) == sizeof(long long) || (LONG_MIN <= val&&val <= LONG_MAX))
				return new obj_long((long)val);
			else
				return new obj_llong(val);
		}
		case 'b': { // b:val;
			if (++beg >= end)
				THROW_INPUT_ERROR("php_load: Incompleted php boolean");
			if (':' != *beg)
				THROW_INPUT_ERROR("php_load: boolean, expect ':'");
			char* pp = NULL;
			long val = strtol(beg+1, &pp, 10);
			if (';' != *pp)
				THROW_INPUT_ERROR2("php_load: boolean, expect ';'", pp);
			beg = pp + 1;
			return new obj_bool(val ? 1 : 0);
		}
#if 0
		case 'd': { // d:val;
			if (++beg >= end)
				THROW_INPUT_ERROR("php_load: Incompleted php double");
			if (':' != *beg)
				THROW_INPUT_ERROR("php_load: double, expect ':'");
			char* pp = NULL;
			double val = strtod(beg+1, &pp);
			if (';' != *pp)
				THROW_INPUT_ERROR("php_load: double, expect ';'");
			beg = pp + 1;
			return new obj_double(val);
		}
#else
		case 'd': { // d:val;
			if (++beg >= end)
				THROW_INPUT_ERROR("php_load: Incompleted php double");
			if (':' != *beg)
				THROW_INPUT_ERROR("php_load: long double, expect ':'");
			char* pp = NULL;
		#ifdef __CYGWIN__ //_LDBL_EQ_DBL
			double val = strtod(beg+1, &pp);
		#else
			long double val = strtold(beg+1, &pp);
		#endif
			if (';' != *pp)
				THROW_INPUT_ERROR2("php_load: long double, expect ';'", pp);
			beg = pp + 1;
			return new obj_ldouble(val);
		}
#endif
		case 's': { // s:size:"content";
			if (++beg >= end)
				THROW_INPUT_ERROR("php_load: Incompleted php string");
			if (':' != *beg)
				THROW_INPUT_ERROR("php_load: string.size expect ':'");
			char* pp = NULL;
			long len = strtol(beg+1, &pp, 10);
			if (':' != *pp)
				THROW_INPUT_ERROR2("php_load: string.content expect ':'", pp);
			if (len < 0)
				THROW_INPUT_ERROR2("php_load: string.size is negtive", beg+1);
			if ('"' != pp[1])
				THROW_INPUT_ERROR2("php_load: string.content expect '\"'", pp+1);
			if ('"' != pp[len+2])
				THROW_INPUT_ERROR2("php_load: string.content not found right quote", pp+len+2);
			if (';' != pp[len+3])
				THROW_INPUT_ERROR2("php_load: string didn't end with ';'", pp+len+3);
			std::unique_ptr<obj_string> x(new obj_string);
			x->resize(len);
			x->assign(pp+2, len);
			beg = pp + len + 4; // s:size:"content";
			return x.release();
		}
		case 'a': { // A:size:{key;value; ....}
		//	fprintf(stderr, "loading array: ");
			if (++beg >= end)
				THROW_INPUT_ERROR("php_load: Incompleted php array");
			if (':' != *beg)
				THROW_INPUT_ERROR("php_load: array.size expect ':'");
			char* pp = NULL;
			long len = strtol(beg+1, &pp, 10);
		//	fprintf(stderr, "size=%ld\n", len);
			if (':' != pp[0] || '{' != pp[1])
				THROW_INPUT_ERROR2("php_load: array.size should followed by ':{'", pp);
			std::unique_ptr<obj_array> arr(new obj_array);
			std::unique_ptr<php_array> map;
			arr->resize(2*len);
			beg = pp + 2;
			long max_idx = -1;
			for (long i = 0; i < len; ++i) {
			//	fprintf(stderr, "loading array[%ld]:\n", i);
				const char* key_pos = beg;
				std::unique_ptr<obj> key(php_load_impl(base, beg, end));
				if (key.get() == NULL) {
					THROW_INPUT_ERROR2("php_load: array.key must not be NULL", key_pos);
				}
				if (arr.get()) {
					obj_long* l = dynamic_cast<obj_long*>(key.get());
					if (l) {
						long idx = l->t;
						if (idx >= 0 && idx < (long)arr->size()) {
							(*arr)[idx].reset(php_load_impl(base, beg, end));
							max_idx = std::max(idx, max_idx);
							continue;
						}
					}
				}
				if (map.get() == NULL)
					map.reset(new php_array);
				if (arr.get()) {
					for (long j = 0; j <= max_idx; ++j) {
						if ((*arr)[j])
							(*map)[obj_ptr(new obj_long(j))] = (*arr)[j];
					}
					arr.reset(NULL);
				}
				(*map)[obj_ptr(key.get())].reset(php_load_impl(base, beg, end));
				key.release();
			}
			if ('}' != *beg)
				THROW_INPUT_ERROR("php_load: array not correctly closed");
			beg += 1;
			if (arr.get()) {
				arr->resize(max_idx+1);
				arr->shrink_to_fit();
				return arr.release();
			}
			return map.release();
		}
		case 'N': {
			if (++beg >= end)
				THROW_INPUT_ERROR("php_load: Incompleted php NULL");
			if (';' != *beg)
				THROW_INPUT_ERROR("php_load: NULL expect ';'");
			beg += 1;
			return NULL;
		}
		case 'O': {
// O:strlen(class name):"class name":fields_num:{s:strlen(field name):"field name":field definition;(repeated per field)}
			if (++beg >= end)
				THROW_INPUT_ERROR("php_load: Incompleted php Object");
			if (':' != *beg)
				THROW_INPUT_ERROR("php_load: Object.namelen expect ':' 1");
			char* pp = NULL;
			long len = strtol(beg+1, &pp, 10);
			if (':' != pp[0] && '"' != pp[1])
				THROW_INPUT_ERROR2("php_load: Object.namelen expect ':\"' 2", pp);
			if (pp + 4 + len > end)
				THROW_INPUT_ERROR2("php_load: Object 3", pp);
			std::unique_ptr<php_object> tree(new php_object);
			tree->cls_name.assign(pp + 2, len);
			long fields = strtol(pp + 4 + len, &pp, 10);
			if (':' != pp[0] || '{' != pp[1])
				THROW_INPUT_ERROR2("php_load: Object 4", pp);
			beg = pp + 2;
			for (long i = 0; i < fields; ++i) {
				std::unique_ptr<obj> pname(php_load_impl(base, beg, end));
				obj_string* name = dynamic_cast<obj_string*>(pname.get());
				if (NULL == name)
					THROW_INPUT_ERROR("object field name is not a string");
				std::unique_ptr<obj> value(php_load_impl(base, beg, end));
				tree->fields[*name].reset(value.release());
			}
			if ('}' != beg[0])
				THROW_INPUT_ERROR("php_load: Object not correctly closed");
			beg += 1;
			return tree.release();
		}
	}
	assert(0);
	return NULL;
}

obj* php_load(const char** beg, const char* end) {
	const char* base = *beg;
	return php_load_impl(base, *beg, end);
}


size_t php_hash::operator()(const obj_ptr& p) const {
	if (p) {
		if (const obj_int* x = dynamic_cast<const obj_int*>(p.get()))
			return x->t;
		if (const obj_string* x = dynamic_cast<const obj_string*>(p.get()))
			return fstring_func::hash()(*x);
		size_t idx = g_meta.find_i(typeid(*p).name());
		if (g_meta.end_i() != idx) {
			size_t (*pf_hash)(const obj*);
			pf_hash = g_meta.val(idx).hash;
			if (pf_hash)
				return pf_hash(p.get());
		}
		std::string msg = "php_array.key_hash: Unsupported key type: ";
		msg.append(typeid(*p).name());
		throw std::runtime_error(msg);
	} else
		return 256 * 'N' + ';'; // NULL object
}

bool php_equal::operator()(const obj_ptr& p, const obj_ptr& q) const {
	if (p && q) {
		{
			const obj_int* x = dynamic_cast<const obj_int*>(p.get());
			const obj_int* y = dynamic_cast<const obj_int*>(q.get());
			if (x && y)
				return x->t == y->t;
			if (!x ^ !y)
				return false;
		}
		{
			const obj_string* x = dynamic_cast<const obj_string*>(p.get());
			const obj_string* y = dynamic_cast<const obj_string*>(q.get());
			if (x && y)
				return x->get_boxed() == y->get_boxed();
			if (!x ^ !y)
				return false;
		}
		size_t idx;
		{
			char bin_name[1024];
			size_t len = make_bin_name(bin_name, sizeof(bin_name), typeid(*p), typeid(*q));
			idx = g_binary_dispatch.find_i(fstring(bin_name, len));
		}
		if (g_binary_dispatch.end_i() != idx) {
			bool (*pf_equal)(const obj*, const obj*);
			pf_equal = g_binary_dispatch.val(idx).equal;
			if (pf_equal)
				return pf_equal(p.get(), q.get());
		}
		std::string msg = "php_array.key_equal: Unsupported keys(p.type=";
		msg.append(typeid(*p).name());
		msg.append(", q.type=");
		msg.append(typeid(*q).name());
		throw std::runtime_error(msg);
	} else
		return p.get() == q.get();
}



} // namespace objbox
} // namespace terark



