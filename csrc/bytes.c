#include "bytes.h"


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
      // List length
      // "Negation" is 0 the top bit.
      int8_t listLength = header & 0x7F;
      memcpy(output, input, listLength);
      input  += listLength;
      output += listLength;
      read   += listLength;
    }
  }
}

// void write_bytes_rle(const uint64_t length, const int8_t* input, int8_t* output) {
//   uint64_t written = 0;
//   int8_t potential_run_start = input;
//   int8_t potential_run_end = input;
//   int8_t potential_run = *input;

//   while (written < length) {
//     int8_t header = *input;
//     input++;

    

//   }
// }
