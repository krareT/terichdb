## TerarkDB
TerarkDB is an open source NoSQL data store based on terark storage engine.

TerarkDB is:

- Read optimized, faster than ever.
- Data is highly compressed and decompressed is not required before read.
- Use FSA and succinct technologies, totally different from B+ and LSM(Which is widely used in other data stores).
- Native support for regex expression query.
- [Benchmark](http://terark.com/zh/blog/detail/2)

## Features
- High Compression Ratio
  - Higher than snappy(2~5 times) and other database products, higher than gzip, sometimes higher than bzip
- Search Directly on Compressed Data without Decompression
  - Compressed data as index, index as compressed data
- Fast Access
  - Optimized for SSD and in-memory workloads
- Flexible Indexing
  - Multiple indexes on one table
  - Unique/non-unique index
  - Composite index(one index on multiple columns)
  - Ordered index (support range query)
- Native regular expression query
- Embeddable & Standalone Database
- Persistent
- Schema Based, with Rich Data Types
- Column Group
- Supported Platforms: Linux, Windows, Mac
- Transparent foundation for developers to build customized products

## Compile TerarkDB

### Dependencies

  - libboost_system.so(1.60.0) (boost_1_60_0)
  - libboost_filesystem(1.60.0)(Could be found in boost_1_60_0)
  - libwiredtiger.so(v2.7.0)
  - libtbb.so(tbb44_20160128)

### Make
Execute `make` command under root dir.

### Compiler Support

- Linux : `g++-4.9`, `g++-5.3`
- OS X : `g++-5.3`, `g++-6.0`, `clang++-7.3`
- Windows : `vs2015`


## License
TerarkDB follows [Free Software Foundation's GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html)

For commercial purposes, please  [contact us](http://www.terark.com).
