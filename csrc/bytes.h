#ifndef __ORC_BYTES_H
#define __ORC_BYTES_H

#include "anemone_base.h"
#include <stdio.h>
#include <string.h>

// ------------------------
// Read Boolean Bytestring
// ------------------------
void read_bytes_rle(const uint64_t length, const uint8_t* input, uint8_t* output);

#endif //__ORC_BYTES_H
