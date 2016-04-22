## 1.TerarkDB
TerarkDB is an open source NoSQL data store based on terark storage engine.

TerarkDB is:

- Read optimized, faster than ever.
- Data is highly compressed and decompressed is not required before read.
- Use FSA and succinct technologies, totally different from B+ and LSM(Which is widely used in other data stores).
- Native support for regex expression query.
- [Benchmark](http://terark.com/zh/blog/detail/2)

## 2.Features
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

## 3.Compile TerarkDB

### 3.1.Dependencies

  - libboost_system.so(1.60.0) (boost_1_60_0)
  - libboost_filesystem(1.60.0)(Could be found in boost_1_60_0)
  - libwiredtiger.so(v2.7.0)
  - libtbb.so(tbb44_20160128)

### 3.2.Compiler Support

- Linux : `g++-4.8`, `g++-4.9`, `g++-5.3`
- OS X : `g++-5.3`, `g++-6.0`, `clang++-7.3`
- Windows : `vs2015`

### 3.3.`dfadb` alternative
1. TerarkDB is open source but our core data structure and algorithm(named `dfadb`) is not yet.
2. `dfadb` is supported by a library named `fsa_all_*` in our released packages.
3. Developers who want to build their own TerarkDB, should place the `fsa_all-*` library into their library search path(e.g. `/usr/local/lib`).
4. Self-compiled TerarkDB could only use limited APIs, and need to set `“TableClass” : “MockDbTable”,` in `dbmeta.json`([TerarkDB's schema file](http://terark.com/zh/docs/1_3)).

## 4.License
TerarkDB follows [Free Software Foundation's GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html)

For commercial purposes, please  [contact us](http://www.terark.com).
