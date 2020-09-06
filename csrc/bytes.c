#include "bytes.h"
#include <string.h>


/* Byte Run Length Encoding
 *
 * For byte streams, ORC uses a very light weight
 * encoding of identical values.
 *
 *   Run - a sequence of at least 3 identical values
 *   Literals - a sequence of non-identical values
 *
 * The first byte of each group of values is a header
 * that determines whether it is a run (value between
 * 0 to 127) or literal list (value between -128 to -1).
 * For runs, the control byte is the length of the run
 * minus the length of the minimal run (3) and the
 * control byte for literal lists is the negative length
 * of the list. For example, a hundred 0’s is encoded as
 * [0x61, 0x00] and the sequence 0x44, 0x45 would be
 * encoded as [0xfe, 0x44, 0x45]. The next group can
 * choose either of the encodings.
 */
void read_bytes_rle(const uint64_t length, const uint8_t* input, uint8_t* output) {
  uint64_t read = 0;

  while (read < length) {
    uint8_t header = *input;
    input++;

    // Note: I'm using unsigned ints to make
    //       to make it safe to add 3.
    if (header < 128) {
      // Run length encoding
      uint8_t runLength = header + 3;
      uint8_t value = *input;
      input++;

      memset(output, value, runLength);
      output += runLength;
      read   += runLength;
    } else {
      // Negation is taking the 2's complement.
      uint8_t listLength = ~header + 1;
      memcpy(output, input, listLength);
      input  += listLength;
      output += listLength;
      read   += listLength;
    }
  }
}


uint64_t write_bytes_rle(const uint64_t length, const uint8_t* input, uint8_t* output) {
  uint64_t written   = 0;
  uint8_t* writehead = output;

  // Pessimistically write literals until a run is found.
  while (written < length) {
    uint8_t* headerpos    = writehead++;
    uint8_t  runvalue     = *(input++);
    uint8_t  litsize      = 1;
    uint16_t runsize      = 0;
    uint8_t  lastmatches  = 0;

    *(writehead++) = runvalue;

    while (litsize < 128 && litsize + written < length) {
      uint8_t towrite = *(input++);
      *(writehead++) = towrite;
      litsize++;

      if (lastmatches && towrite == runvalue) {
        runsize    = 3;
        litsize   -= runsize;
        writehead -= runsize;
        break;
      }

      lastmatches = runvalue == towrite;
      runvalue = towrite;
    }

    if (litsize > 0) {
      *headerpos = ~litsize + 1;
      written += litsize;
    } else {
      writehead = headerpos;
    }

    if (runsize > 0) {
      while (runsize + written < length && runsize < 130) {
        uint8_t next = *input;
        if (next != runvalue)
          break;

        runsize++;
        input++;
      }

      *(writehead++) = (uint8_t) (runsize - 3);
      *(writehead++) = runvalue;
      written += runsize;
    }
  }
  return (writehead - output);
}


uint64_t readLongs(const uint8_t *data_input, uint64_t len, uint64_t bitSize, uint64_t *data_output) {
  uint64_t ret = 0;
  uint64_t curByte = 0;
  uint64_t bitsLeft = 0;

  // TODO: unroll to improve performance
  for(uint64_t i = 0; i < len; i++) {
    uint64_t result = 0;
    uint64_t bitsLeftToRead = bitSize;
    while (bitsLeftToRead > bitsLeft) {
      result <<= bitsLeft;
      result |= curByte & ((1 << bitsLeft) - 1);
      bitsLeftToRead -= bitsLeft;
      curByte = *(data_input++);
      bitsLeft = 8;
    }

    // handle the left over bits
    if (bitsLeftToRead > 0) {
      result <<= bitsLeftToRead;
      bitsLeft -= (uint32_t)(bitsLeftToRead);
      result |= (curByte >> bitsLeft) & ((1 << bitsLeftToRead) - 1);
    }
    data_output[i] = result;
    ++ret;
  }

  return ret;
}
