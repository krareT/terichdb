public:
	MyType& operator<<(         char x) { this->writeByte((byte)x); return *this; }
	MyType& operator<<(  signed char x) { this->writeByte((byte)x); return *this; }
	MyType& operator<<(unsigned char x) { this->writeByte((byte)x); return *this; }

	MyType& save(const          char* s, size_t n) { this->ensureWrite(s, n); return *this; }
	MyType& save(const   signed char* s, size_t n) { this->ensureWrite(s, n); return *this; }
	MyType& save(const unsigned char* s, size_t n) { this->ensureWrite(s, n); return *this; }

	MyType& operator<<(const char* s)
	{
		var_size_t n(strlen(s));
		*this << n;
		this->ensureWrite(s, n.t);
		return *this;
	}
	MyType& operator<<(const wchar_t* s)
	{
		var_size_t n(wcslen(s));
		*this << n;
		this->save(s, n.t);
		return *this;
	}

	template<class CharType, class Traits, class Allocator>
	MyType& operator<<(const std::basic_string<CharType, Traits, Allocator>& x)
	{
		var_size_t length(x.size());
		*this << (length);
		this->save(x.data(), length.t);
		return *this;
	}

	MyType& operator<<(const std::string& x)
	{
	#if 0
		var_size_t length(x.size());
		*this << (length);
		this->save(x.data(), length.t);
	#else
		this->getStream()->write_string(x);
	#endif
		return *this;
	}

	MyType& operator<<(const std::wstring& x)
	{
		var_size_t length(x.size());
		*this << (length);
		this->save(x.data(), length.t);
		return *this;
	}

