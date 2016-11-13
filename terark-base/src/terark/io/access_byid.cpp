/* vim: set tabstop=4 : */
#include "access_byid.hpp"
#include <algorithm>
#include <stdexcept>
#include <terark/num_to_str.hpp>
#include <terark/util/throw.hpp>

#if defined(_MSC_VER) && (defined(_DEBUG) || !defined(NDEBUG))
//	#undef  TERARK_RPC_DONT_USE_HASH_STRMAP
	#define TERARK_RPC_DONT_USE_HASH_STRMAP
#endif

#ifdef TERARK_RPC_DONT_USE_HASH_STRMAP
	#include <map>
	typedef std::map<std::string, void*> NameMapBase;
#else
	#include <terark/hash_strmap.hpp>
	typedef terark::hash_strmap<void*> NameMapBase;
#endif

namespace terark {
	class NameMapForAccessByNameID : public NameMapBase {};

	id_generator::id_generator(uintptr_t maxID)
		: id_list(maxID+1)
	{
		chain(0);
		m_nUsed = 0;
	}
	id_generator::~id_generator()
	{
		// must call this->clear() before destruct
		// otherwise, these assertion will fail!
		assert(0 == m_nUsed);
		assert(id_list.empty());
	}

	void id_generator::chain(uintptr_t newHead)
	{
		for (uintptr_t i = newHead; i != id_list.size()-1; ++i)
			id_list[i] = i + 1; // id_list[i] link to id_list[i+1]
		id_list.back() = 0; // set 0 as tail
	}

	uintptr_t id_generator::alloc_id()
	{
		if (0 == id_list[0]) // out of space, enlarge id_list
		{
			uintptr_t newHead = id_list[0] = id_list.size();
			id_list.resize(id_list.size()*2);
			chain(newHead);
		}
		++m_nUsed;
		uintptr_t retid = id_list[0];
		id_list[0] = id_list[retid];
		return retid;
	}

	void id_generator::free_id(uintptr_t id)
	{
		assert(id >= 1);
		assert(id <= uintptr_t(id_list.size()));
		assert(m_nUsed >= 1);
		if (uintptr_t(id_list.size()) < id || id <= 0)
		{
			throw std::invalid_argument("void id_generator::free_id(uintptr_t id)");
		}
		id_list[id] = id_list[0];
		id_list[0] = id;
		--m_nUsed;
	}

	uintptr_t id_generator::add_val(uintptr_t val)
	{
		uintptr_t id = alloc_id();
		id_list[id] = val;
		return id;
	}

	void id_generator::get_used_id(std::vector<uintptr_t>* used_id) const
	{
		assert(NULL != used_id);
		used_id->resize(0);
		used_id->reserve(id_list.size());
		std::vector<uintptr_t> free_subscript;
		free_subscript.reserve(id_list.size());
		free_subscript.push_back(0);
		for (uintptr_t h = id_list[0]; h != 0; h = id_list[h])
			free_subscript.push_back(h);
		std::sort(free_subscript.begin(), free_subscript.end());
		free_subscript.push_back(id_list.size());
		for (uintptr_t i = 0; i != free_subscript.size()-1; ++i)
		{
			uintptr_t d1 = free_subscript[i+0]+1;
			uintptr_t d2 = free_subscript[i+1];
			for (uintptr_t j = d1; j < d2; ++j)
				used_id->push_back(j);
		}
	}

void* access_byid::get_ptr_imp(uintptr_t id, const char* func) const
{
	if (this->is_valid(id))
	{
		assert(this->get_val(id) > 1024);
		return (void*)(this->get_val(id));
	}
	string_appender<> oss;
	oss << func << ": id too large";
	throw std::invalid_argument(oss.str());
}

void access_byid::on_destroy(void* vp)
{
	::free(vp);
}

void access_byid::destroy()
{
	std::vector<uintptr_t> used_id;
	get_used_id(&used_id);
	for (uintptr_t i = 0; i != used_id.size(); ++i)
	{
		void* x = (void*)this->get_val(used_id[i]);
		on_destroy(x);
	}
	this->clear();
}

access_byid::~access_byid()
{
	// must called this->destroy() before destruct
	assert(0 == this->size());
}

AccessByNameID<void*>::AccessByNameID() {
	m_byname = new NameMapForAccessByNameID; // must success
}

uintptr_t AccessByNameID<void*>::add_ptr(void* x, const std::string& name, void** existed)
{
	assert(NULL != x);
	void*& y = (*m_byname)[name];
	if (y) {
		*existed = y;
		return 0;
	}
	else {
		*existed = NULL;
		y = x;
		uintptr_t id = m_byid.add_ptr(x);
		return id;
	}
}

void* AccessByNameID<void*>::get_byname(const std::string& name) const
{
	NameMapForAccessByNameID::const_iterator iter = m_byname->find(name);
	if (m_byname->end() != iter)
		return iter->second;
	else
		return 0;
}

void AccessByNameID<void*>::on_destroy(void* vp)
{
	::free(vp);
}

//! delete all object in list, and clear self
void AccessByNameID<void*>::destroy()
{
	std::vector<uintptr_t> used_id;
	m_byid.get_used_id(&used_id);
	std::vector<uintptr_t> bynamep(m_byname->size());
	NameMapForAccessByNameID::iterator iter = m_byname->begin();
	for (uintptr_t i = 0; iter != m_byname->end(); ++iter)
		bynamep[i++] = (uintptr_t)iter->second;
	for (uintptr_t i = 0; i != used_id.size(); ++i)
		used_id[i] = m_byid.get_val(used_id[i]);
	std::sort(used_id.begin(), used_id.end());
	std::sort(bynamep.begin(), bynamep.end());
	uintptr_t n = std::set_union(used_id.begin(), used_id.end(),
		bynamep.begin(), bynamep.end(), used_id.begin()) - used_id.begin();
	assert(used_id.size() == n);
	if (used_id.size() != n) {
		THROW_STD(runtime_error,
			"(used_id.size=%zd) != (n=%zd)", used_id.size(), n);
	}
	for (uintptr_t i = 0; i != n; ++i)
		on_destroy((void*)used_id[i]);
	m_byid.clear();
	m_byname->clear();
}

void AccessByNameID<void*>::remove(uintptr_t id, const std::string& name)
{
	void* p = m_byid.get_ptr(id);
	m_byid.free_id(id);
	m_byname->erase(name);
	on_destroy(p);
}

void AccessByNameID<void*>::remove(uintptr_t id)
{
	void* p = m_byid.get_ptr(id);
	m_byid.free_id(id);
	on_destroy(p);
}

bool AccessByNameID<void*>::check_id(uintptr_t id, const char* szClassName, std::string& err) const
{
	if (terark_unlikely(0 == id))
	{
		assert(0);
	}
	if (!this->is_valid(id))
	{
		string_appender<> oss;
		oss << "can not find " << szClassName << " object[id=" << id << "]"
			;
		err = oss.str();
		return false;
	}
	return true;
}

AccessByNameID<void*>::~AccessByNameID()
{
	// must call destroy() before destructor was called
	assert(m_byid.size() == 0);
	delete m_byname;
}


} // namespace terark

