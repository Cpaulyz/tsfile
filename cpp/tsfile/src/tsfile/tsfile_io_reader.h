
#ifndef STORAGE_TSFILE_TSFILE_IO_REAER_H
#define STORAGE_TSFILE_TSFILE_IO_REAER_H

#include "common/db_utils.h"
#include "common/tsblock/tsblock.h"
#include "tsfile/storage_utils.h"
#include "read_file.h"
#include "tsfile_common.h"
#include "tsfile_series_scan_iterator.h"
#include "chunk_reader.h"
#include "tsfile/filter/filter.h"

namespace timecho
{
namespace storage
{

class TsFileSeriesScanIterator;

/* 
 * TODO:
 * TsFileIOReader correspond to one tsfile.
 * It may be shared by many query.
 */
class TsFileIOReader
{
public:
  TsFileIOReader() : read_file_(nullptr),
                     tsfile_meta_page_arena_(),
                     tsfile_meta_(&tsfile_meta_page_arena_),
                     tsfile_meta_ready_(false),
                     read_file_created_(false)
  {
    tsfile_meta_page_arena_.init(512, common::MOD_DEFAULT);
  }

  int init(const std::string &file_path);
  int init(ReadFile *read_file);
  void reset();

  int alloc_ssi(const std::string &device_path,
                const std::string &measurement_name,
                TsFileSeriesScanIterator *&ssi,
                Filter *time_filter = nullptr);
  void revert_ssi(TsFileSeriesScanIterator *ssi);
  std::string get_file_path() const { return read_file_->file_path(); }
private:
  FORCE_INLINE int32_t file_size() const { return read_file_->file_size(); }
  int load_tsfile_meta();
  int load_tsfile_meta_if_necessary();
  int load_device_index_entry(const std::string &device_path,
                              MetaIndexEntry &device_index_entry,
                              int64_t &end_offset);
  int load_measurement_index_entry(const std::string &measurement_name,
                                   int64_t start_offset,
                                   int64_t end_offset,
                                   MetaIndexEntry &ret_measurement_index_entry,
                                   int64_t &ret_end_offset);
  int do_load_timeseries_index(const std::string &measurement_name_str,
                               int64_t start_offset,
                               int64_t end_offset,
                               common::PageArena &pa,
                               TimeseriesIndex &ts_index);
  int load_timeseries_index_for_ssi(const std::string &device_path,
                                    const std::string &measurement_name,
                                    TsFileSeriesScanIterator *&ssi);
  int search_from_leaf_node(const common::String &target_name,
                            MetaIndexNode *index_node,
                            MetaIndexEntry &ret_index_entry,
                            int64_t &ret_end_offset);
  int search_from_internal_node(const common::String &target_name,
                                MetaIndexNode *index_node,
                                MetaIndexEntry &ret_index_entry,
                                int64_t &ret_end_offset);
  bool filter_stasify(TimeseriesIndex &ts_index, Filter *time_filter);

private:
  ReadFile *read_file_;
  common::PageArena tsfile_meta_page_arena_;
  TsFileMeta tsfile_meta_;
  bool tsfile_meta_ready_;
  bool read_file_created_;
};

// class TsFileIOReaderSet
// {
// public:
//   TsFileIOReaderSet() : readers_() {}
//   ~TsFileIOReaderSet() {}
// 
//   static TsFileIOReaderSet& get_instance()
//   {
//     static TsFileIOReaderSet g_tfrs;
//     return g_tfrs;
//   }
// 
//   int get_or_create(const std::string &file_path,
//                     ReadFile *read_file,
//                     TsFileIOReader *&reader)
//   {
//     ASSERT(read_file != nullptr);
//     do {
//       {
//         MutexGuard g(&mutex_);
//         MapIterator find_iter = readers_.find(file_path);
//         if (find_iter != readers_.end()) {
//           reader = find_iter->second;
//           reader->ref();
//           ASSERT(reader->ref() >= 2);
//           return common::E_OK;
//         }
//       }
// 
//       reader = new TsFileIOReader;
//       reader->ref();
//       ASSERT(reader->get_ref() == 1);
//       reader->init(read_file);
// 
//       MutexGuard g(&mutex_);
//       std::pair<MapIterator, bool> ins_res = readers_.insert(std::make_pair(file_path, reader));
//       if (ins_res.second) {
//         return common::E_OK;
//       } else {
//         delete reader;
//         continue;
//       }
//     } while (false);
//   }
// 
//   void revert(TsFileIOReader *reader)
//   {
//     const std::string &file_path = reader->get_file_path();
//     MutexGuard g(&mutex_);
//     MapIterator find_iter = readers_.find(file_path);
//     if (find_iter != readers_.end()) {
//       reader = find_iter->second;
//       if (0 == reader->unref()) {
//         readers_.erase(file_path);
//         delete reader;
//       }
//     } else {
//       ASSERT(false);
//     }
//   }
// 
// private:
//   typedef std::map<std::string, TsFileIOReader*>::iterator MapIterator;
// private:
//   std::map<std::string, TsFileIOReader*> readers_;
//   common::Mutex mutex_;
// };

} // end namespace storage
} // end namespace timecho

#endif

