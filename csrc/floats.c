#include "floats.h"

// Reads uint32vector from little endian byte array.
void read_le_uint32(const uint64_t length, const uint8_t* input, uint32_t* output) {
  uint32_t tmp = 0;
  for (uint64_t i = 0; i < length; i++) {
    tmp  = ((uint32_t) *(input++));
    tmp |= ((uint32_t) *(input++)) << 8;
    tmp |= ((uint32_t) *(input++)) << 16;
    tmp |= ((uint32_t) *(input++)) << 24;
    output[i] = tmp;
  }
}

// Reads uint64 vector from little endian byte array.
void read_le_uint64(const uint64_t length, const uint8_t* input, uint64_t* output) {
  uint64_t tmp = 0;
  for (uint64_t i = 0; i < length; i++) {
    tmp  = ((uint64_t) *(input++));
    tmp |= ((uint64_t) *(input++)) << 8;
    tmp |= ((uint64_t) *(input++)) << 16;
    tmp |= ((uint64_t) *(input++)) << 24;
    tmp |= ((uint64_t) *(input++)) << 32;
    tmp |= ((uint64_t) *(input++)) << 40;
    tmp |= ((uint64_t) *(input++)) << 48;
    tmp |= ((uint64_t) *(input++)) << 56;
    output[i] = tmp;
  }
}
