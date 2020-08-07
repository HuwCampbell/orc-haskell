#ifndef __ORC_FLOAT_H
#define __ORC_FLOAT_H

#include <stdint.h>
#include <strings.h>

// ------------------------
// Read Boolean Bytestring
// ------------------------
void read_le_uint32(const uint64_t length, const uint8_t* input, uint32_t* output) ;

void read_le_uint64(const uint64_t length, const uint8_t* input, uint64_t* output) ;

#endif //__ORC_FLOAT_H
