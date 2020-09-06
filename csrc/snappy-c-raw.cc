//
// This file is not part of snappy, it is used by snooker to expose a lower
// level API than what is available from C.
//

#include "snappy.h"
#include "snappy-c.h"

extern "C" {

void snappy_raw_compress(const char* input,
                         size_t input_length,
                         char* compressed,
                         size_t *compressed_length) {
  snappy::RawCompress(input, input_length, compressed, compressed_length);
}

snappy_status snappy_raw_uncompress(const char* compressed,
                                    size_t compressed_length,
                                    char* uncompressed) {
  if (snappy::RawUncompress(compressed, compressed_length, uncompressed)) {
    return SNAPPY_OK;
  } else {
    return SNAPPY_INVALID_INPUT;
  }
}

}  // extern "C"
