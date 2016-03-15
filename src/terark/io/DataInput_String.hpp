
	MyType& operator>>(         char& x) { x = (         char)getStream()->readByte(); return *this; }
	MyType& operator>>(unsigned char& x) { x = (unsigned char)getStream()->readByte(); return *this; }
	MyType& operator>>(  signed char& x) { x = (  signed char)getStream()->readByte(); return *this; }
	
	MyType& load(         char* s, size_t n) { this->ensureRead(s, n); return *this; }
	MyType& load(unsigned char* s, size_t n) { this->ensureRead(s, n); return *this; }
	MyType& load(  signed char* s, size_t n) { this->ensureRead(s, n); return *this; }

	MyType& operator>>(std::string& x)
   	{
	   	this->getStream()->read_string(x);
		return *this;
   	}
//	MyType& operator>>(std:: string& x) { return load_s1(x); }
	MyType& operator>>(std::wstring& x) { return load_s1(x); }

private:
	//! string in file format: [length : ....content.... ]
	template<class CharType, class Traits, class Allocator>
	MyType& load_s1(std::basic_string<CharType, Traits, Allocator>& x)
	{
		var_size_t length;
		*this >> length;
		x.resize(length.t); // str will be allocated at least (length+1) chars..
		if (terark_likely(length.t)) {
		//	CharType* data = const_cast<CharType*>(str.data());
			CharType* data = &*x.begin(); // this will make a mutable string content
			this->load(data, length.t);
		//	data[length.t] = 0; // in most string implementation, this is accessible
		//	data[length.t] = 0; // in some string implementation, this is out of string bound
		}
		return *this;
	}

#ifdef TERARK_DATA_IO_ENABLE_LOAD_RAW_CHAR_PTR
public:
	MyType& operator>>(char*& s) { return load_s0(s); }
	MyType& operator>>(wchar_t*& s) { return load_s0(s); }
private:
	template<class ChT> MyType& load_s0(ChT*& s)
	{
		assert(0 == s);
		var_size_t n;
		*this >> n;
		s = new ChT[n.t+1];
		this->load(s, n.t);
		s[n] = 0;
		return *this;
	}
#endif


