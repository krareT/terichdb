#ifndef __nark_db_seg_db_hpp__
#define __nark_db_seg_db_hpp__

#include "db_table.hpp"

// 先只针对 NestLoudsTrie 的特性设计接口
// NestLoudsTrie 的特性:
//   * 从 RecID 获取数据，需要一个 UintVector 保存 RecID 到 LeafNodeID 的映射
//   * 可以从 root 开始，搜索 PrimaryKey, 匹配到 PrimaryKey 之后，一路向前走到 Leaf，
//     向前的过程中就同时得到了 Data。但无法在不需要额外内存的前提下得到 RecID
//   * 在匹配到 PrimaryKey 之后，前往 Leaf 的过程中，可以跳过解压 LinkString 的过程，
//     使得到达 Leaf 的速度大大加快。这提供了一个以较小代价实现 PrimaryKey to RecID
//     映射的方法: 使用 NestTrieDAWG， PrimaryKey to DawgIndex 需要付出 TermFlag
//     的代价，再付出一个 DawgIndex to RecID 数组(UintVector)的代价，就可以实现
//     PrimaryKey to RecID mapping；但这样就不能一个 DataIndex 配多个 DataStore，
//     虽然可以通过 LazyUnionDFA 实现与 One Index Many Store 相近的性能，这必须先将
//     整个数据集排序，再按顺序分区创建多个 NestTrieDAWG
//   * 结论：PrimaryKey 仍然使用单独的 Index，一个 PrimaryIndex 对应多个 DataStore
//          DataStore 中如果不保存 PrimaryKey，就需要从 PrimaryIndex 中根据 RecID
//          获取 PrimaryKey，这需要一个与 PrimaryIndex RowNum 同等长度的数组，
//          数组元素是 PrimaryIndexTrie 的 LeafNodeID

namespace terark { namespace db {

class DataBase : public RefCounter {
	hash_strmap<CompositeTablePtr> m_tables;
	std::string m_dbDir;

public:
	void openDb(fstring dbDir);

	CompositeTablePtr createTable(fstring tableName, fstring jsonSchema);
	CompositeTablePtr openTable(fstring tableName);

	void dropTable(fstring tableName);
};
typedef boost::intrusive_ptr<DataBase> DataBasePtr;

} } // namespace terark::db

#endif // __nark_db_seg_db_hpp__
