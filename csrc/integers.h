#ifndef __ORC_INTEGERS_H
#define __ORC_INTEGERS_H

#include <stdint.h>
#include <strings.h>

// ------------------------
// Read Boolean Bytestring
// ------------------------

uint64_t write_uint8_rle(const uint64_t length, const uint8_t* input, uint8_t* output);
uint64_t write_uint16_rle(const uint64_t length, const uint16_t* input, uint8_t* output);
uint64_t write_uint32_rle(const uint64_t length, const uint32_t* input, uint8_t* output);
uint64_t write_uint64_rle(const uint64_t length, const uint64_t* input, uint8_t* output);

uint64_t write_int8_rle(const uint64_t length, const int8_t* input, uint8_t* output);
uint64_t write_int16_rle(const uint64_t length, const int16_t* input, uint8_t* output);
uint64_t write_int32_rle(const uint64_t length, const int32_t* input, uint8_t* output);
uint64_t write_int64_rle(const uint64_t length, const int64_t* input, uint8_t* output);


#endif //__ORC_INTEGERS_H
