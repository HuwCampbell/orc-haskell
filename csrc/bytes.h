#ifndef __ORC_BYTES_H
#define __ORC_BYTES_H

#include <stdint.h>
#include <strings.h>

// ------------------------
// Read Boolean Bytestring
// ------------------------
void read_bytes_rle(const uint64_t length, const uint8_t* input, uint8_t* output);

uint64_t readLongs(const uint8_t *data_input, uint64_t len, uint64_t bitSize, uint64_t *data_output);

#endif //__ORC_BYTES_H
