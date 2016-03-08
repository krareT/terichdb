#ifndef __terark_FileDataIO_hpp__
#define __terark_FileDataIO_hpp__

#include "DataIO.hpp"
#include "FileStream.hpp"
#include "StreamBuffer.hpp"

namespace terark {
	template<class DataIO>
	class FileDataInput : public DataIO {
	public:
		FileStream file;
		FileDataInput(const char* fname) : file(fname, "rb") {
			this->attach(&file);
		}
	};
	template<class DataIO>
	class FileDataOutput : public DataIO {
	public:
		FileStream file;
		FileDataOutput(const char* fname) : file(fname, "wb") {
			this->attach(&file);
		}
		~FileDataOutput() {
			this->flush();
			this->attach(NULL);
		}
	};

	typedef FileDataInput<NativeDataInput<InputBuffer> > NativeFileDataInput;
	typedef FileDataOutput<NativeDataOutput<OutputBuffer> > NativeFileDataOutput;
	typedef FileDataInput<PortableDataInput<InputBuffer> > PortableFileDataInput;
	typedef FileDataOutput<PortableDataOutput<OutputBuffer> > PortableFileDataOutput;

	typedef FileDataInput<BigEndianDataInput<InputBuffer> > BigEndianFileDataInput;
	typedef FileDataOutput<BigEndianDataOutput<OutputBuffer> > BigEndianFileDataOutput;
	typedef FileDataInput<LittleEndianDataInput<InputBuffer> > LittleEndianFileDataInput;
	typedef FileDataOutput<LittleEndianDataOutput<OutputBuffer> > LittleEndianFileDataOutput;
}

#endif // __terark_FileDataIO_hpp__

