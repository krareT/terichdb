#ifndef __terark_ptree_hpp__
#define __terark_ptree_hpp__

#include "objbox.hpp"
#include <terark/hash_strmap.hpp>
#include <terark/gold_hash_map.hpp>
//#include <tr1/unordered_map>
#include <terark/util/function.hpp>

namespace terark {
	namespace objbox {

	class TERARK_DLL_EXPORT ptree : public obj {
	public:
		hash_strmap<obj_ptr> children;
		hash_strmap<obj_ptr> attributes;

		~ptree();

		const ptree& get_boxed() const { return *this; }
		      ptree& get_boxed()       { return *this; }

		obj* get(fstring path) const;

		fstring     get(fstring path, fstring     Default) const;
		long long   get(fstring path, long long   Default) const;
		long double get(fstring path, long double Default) const;

		obj_ptr* add(fstring path);
		obj_ptr* set(fstring path, const function<bool(fstring, obj**)>& on_node);
		void set(fstring path, const std::string& val);
		void set(fstring path, const char* val);
		void set(fstring path, fstring     val);
		void set(fstring path, long long   val);
		void set(fstring path, long double val);

		void for_each(fstring base_dir, const function<void(fstring name, obj&)>& op);
	private:
		void for_each_loop(std::string& path, const function<void(fstring name, obj&)>& op);
	};

	class TERARK_DLL_EXPORT php_object : public obj {
	public:
		hash_strmap<obj_ptr> fields;
		std::string cls_name;
		php_object();
		~php_object();
		const php_object& get_boxed() const { return *this; }
			  php_object& get_boxed()       { return *this; }
	};

	struct TERARK_DLL_EXPORT php_hash {
		size_t operator()(const obj_ptr&) const;
	};
	struct TERARK_DLL_EXPORT php_equal {
		bool operator()(const obj_ptr&, const obj_ptr&) const;
	};
	// php_array is a general associated array
	typedef gold_hash_map<obj_ptr, obj_ptr, php_hash, php_equal> php_array_base;
	TERARK_BOXING_OBJECT_DERIVE(php_array, php_array_base)

	template<> struct obj_val<     ptree> { typedef      ptree type; };
	template<> struct obj_val<php_object> { typedef php_object type; };

	TERARK_DLL_EXPORT obj* php_load(const char** beg, const char* end);
	TERARK_DLL_EXPORT void php_save(const obj* self, std::string* out);
	TERARK_DLL_EXPORT void php_append(const obj* self, std::string* out);

} // namespace objbox

} // namespace terark

#endif // __terark_ptree_hpp__

