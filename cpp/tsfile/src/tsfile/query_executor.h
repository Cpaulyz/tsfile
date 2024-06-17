#ifndef STORAGE_TSFILE_READ_QUERY_QUERY_EXECUTOR_H
#define STORAGE_TSFILE_READ_QUERY_QUERY_EXECUTOR_H

#include <vector>

#include "tsfile_series_scan_iterator.h"
#include "read_file.h"
#include "row_record.h"
#include "expression.h"

namespace timecho
{
namespace storage
{

class QueryExecutor
{
public:
  QueryExecutor()
  {
    query_exprs_ = nullptr;
  }

  virtual ~QueryExecutor()
  {
    int size = data_scan_iter_.size();
    for (int i = 0; i < size; ++i) {
      delete data_scan_iter_[i];
      data_scan_iter_[i] = nullptr;
    }
  }

  // virtual int init(QueryExpression *query_expr, ReadFile *read_file) { ASSERT(false); return 0; };

  virtual RowRecord* execute() { ASSERT(false); return nullptr; };

  virtual void end() { ASSERT(false); };

protected:
  QueryExpression                         *query_exprs_;
  std::vector<TsFileSeriesScanIterator*>  data_scan_iter_;
  std::vector<common::TsBlock*>           tsblocks_;
  std::vector<common::ColIterator*>       time_iters_;
  std::vector<common::ColIterator*>       value_iters_;
};

}  // storage
}  // timecho

#endif  // STORAGE_TSFILE_READ_QUERY_QUERY_EXECUTOR_H
