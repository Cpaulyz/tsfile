#ifndef STORAGE_TSFILE_COMPRESS_LZ4_COMPRESSOR_H
#define STORAGE_TSFILE_COMPRESS_LZ4_COMPRESSOR_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include "lz4.h"
#include "compressor.h"
#include "common/errno_define.h"
#include "common/util_define.h"
#include "common/logger/elog.h"
#include "common/allocator/byte_stream.h"

#define UNCOMPRESSED_TIME 4

namespace timecho
{
namespace storage
{

class LZ4Compressor : public Compressor
{
public:
  LZ4Compressor() : compressed_buf_(nullptr), uncompressed_buf_(nullptr) {};
  ~LZ4Compressor() {};
  // @for_compress
  //  true  - for compressiom
  //  false - for uncompression
  int reset(bool for_compress) OVERRIDE;
  void destroy() OVERRIDE;
  int compress(char *uncompressed_buf, 
              uint32_t uncompressed_buf_len, 
              char *&compressed_buf, 
              uint32_t &compressed_buf_len) OVERRIDE;
  void after_compress(char *compressed_buf) OVERRIDE;
  int uncompress(char *compressed_buf,
                uint32_t compressed_buf_len, 
                char *&uncompressed_buf, 
                uint32_t &uncompressed_buf_len) OVERRIDE;
  void after_uncompress(char *uncompressed_buf) OVERRIDE;

private:
  char* compressed_buf_;
  char* uncompressed_buf_;

  int uncompress(char *compressed_buf, 
                uint32_t compressed_buf_len, 
                char *&uncompressed_buf, 
                uint32_t &uncompressed_buf_len, 
                float ratio);
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_COMPRESS_LZ4_COMPRESSOR_H

