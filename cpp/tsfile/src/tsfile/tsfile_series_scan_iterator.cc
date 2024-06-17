
#include "tsfile_series_scan_iterator.h"

using namespace timecho::common;

namespace timecho
{
namespace storage
{

void TsFileSeriesScanIterator::destroy()
{
  timeseries_index_pa_.destroy();
  chunk_reader_.destroy();
  if (tsblock_ != nullptr) {
    delete tsblock_;
    tsblock_ = nullptr;
  }
}

int TsFileSeriesScanIterator::get_next(TsBlock *&ret_tsblock, bool alloc, Filter *oneshoot_filter)
{
  // TODO @filter
  int ret = E_OK;
  Filter *filter = (oneshoot_filter != nullptr) ? oneshoot_filter : time_filter_;
  if (!chunk_reader_.has_more_data()) {
    while (true) {
      if (!has_next_chunk()) {
        return E_NO_MORE_DATA;
      } else {
        advance_to_next_chunk();
        ChunkMeta *cm = get_current_chunk_meta();
        if (filter != nullptr
            && cm->statistic_ != nullptr
            && !filter->satisfy(cm->statistic_)) {
          continue;
        }
        chunk_reader_.reset();
        if (RET_FAIL(chunk_reader_.load_by_meta(cm))) {
        }
      }
    }
  }
  if (IS_SUCC(ret)) {
    if (alloc) {
      ret_tsblock = alloc_tsblock();
    }
    ret = chunk_reader_.get_next_page(ret_tsblock, oneshoot_filter);
  }
  return ret;
}

void TsFileSeriesScanIterator::revert_tsblock()
{
  if (tsblock_ == nullptr) {
    return;
  }
  delete tsblock_;
  tsblock_ = nullptr;
}

int TsFileSeriesScanIterator::init_chunk_reader()
{
  int ret = E_OK;
  chunk_meta_cursor_ = timeseries_index_.get_chunk_meta_list()->begin();
  ChunkMeta *cm = chunk_meta_cursor_.get();
  ASSERT(!chunk_reader_.has_more_data());
  if (RET_FAIL(chunk_reader_.init(read_file_,
                                  timeseries_index_.get_measurement_name(),
                                  timeseries_index_.get_data_type(),
                                  time_filter_))) {
  } else if (RET_FAIL(chunk_reader_.load_by_meta(cm))) {
  } else {
    chunk_meta_cursor_++;
  }
  return ret;
}

TsBlock *TsFileSeriesScanIterator::alloc_tsblock()
{
  ChunkHeader &ch = chunk_reader_.get_chunk_header();
  TsID dummy_ts_id;

  // TODO config
  ColumnDesc time_cd(timecho::common::INT64, TS_2DIFF, SNAPPY, INVALID_TTL, "time", dummy_ts_id);
  ColumnDesc value_cd(ch.data_type_, ch.encoding_type_, ch.compression_type_,
                      INVALID_TTL, ch.measurement_name_, dummy_ts_id);

  tuple_desc_.push_back(time_cd);
  tuple_desc_.push_back(value_cd);

  tsblock_ = new TsBlock(&tuple_desc_);
  if (E_OK != tsblock_->init()) {
    delete tsblock_;
    tsblock_ = nullptr;
  }
  return tsblock_;
}

} // end namespace storage
} // end namespace timecho

