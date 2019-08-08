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

// Map bit length i to closest aligned fixed bit width that can contain i bits.
// const uint8_t ClosestAlignedFixedBitsMap[65] = {
//     1, 1, 2, 4, 4, 8, 8, 8, 8, 16, 16, 16, 16, 16, 16, 16, 16, 24, 24, 24, 24, 24, 24, 24, 24,
//     32, 32, 32, 32, 32, 32, 32, 32,
//     40, 40, 40, 40, 40, 40, 40, 40,
//     48, 48, 48, 48, 48, 48, 48, 48,
//     56, 56, 56, 56, 56, 56, 56, 56,
//     64, 64, 64, 64, 64, 64, 64, 64
// };

// inline uint32_t getClosestAlignedFixedBits(uint32_t n) {
//   if (n <= 64) {
//     return ClosestAlignedFixedBitsMap[n];
//   } else {
//     return 64;
//   }
// }

// void writeInts(int64_t* input, uint32_t offset, size_t len, uint32_t bitSize) {
//   if(len < 1 || bitSize < 1) {
//       return;
//   }

//   if (getClosestAlignedFixedBits(bitSize) == bitSize) {
//     uint32_t numBytes;
//     uint32_t endOffSet = (uint32_t) (offset + len);
//     if (bitSize < 8 ) {;
//       char bitMask = (char)((1 << bitSize) - 1);
//       uint32_t numHops = 8 / bitSize;
//       uint32_t remainder = (uint32_t)(len % numHops);
//       uint32_t endUnroll = endOffSet - remainder;
//       for (uint32_t i = offset; i < endUnroll; i+=numHops) {
//         char toWrite = 0;
//         for (uint32_t j = 0; j < numHops; ++j) {
//           toWrite |= (char)((input[i+j] & bitMask) << (8 - (j + 1) * bitSize));
//         }
//         writeByte(toWrite);
//       }

//       if (remainder > 0) {
//         uint32_t startShift = 8 - bitSize;
//         char toWrite = 0;
//         for (uint32_t i = endUnroll; i < endOffSet; ++i) {
//           toWrite |= (char)((input[i] & bitMask) << startShift);
//           startShift -= bitSize;
//         }
//         writeByte(toWrite);
//       }

//     } else {
//       numBytes = bitSize / 8;

//       for (uint32_t i = offset; i < endOffSet; ++i) {
//         for (uint32_t j = 0; j < numBytes; ++j) {
//           char toWrite = (char)((input[i] >> (8 * (numBytes - j - 1))) & 255);
//           writeByte(toWrite);
//         }
//       }
//     }

//     return;
//   }
