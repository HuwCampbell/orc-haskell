#include "integers.h"


/*
Length(arr)
*/

#define INT(SIZE)  int##SIZE##_t
#define WORD(SIZE) uint##SIZE##_t

#define ZIGZAG(SIZE)                                              \
WORD(SIZE) zigzag##SIZE (INT(SIZE) n) {                           \
  return (WORD(SIZE)) (n << 1) ^ (n >> (SIZE - 1));               \
}

#define PUT_VARWORD_IMPL(SIZE)                                    \
uint8_t put_var_uint##SIZE##_t (WORD(SIZE) chunk, uint8_t* out) { \
  uint8_t cnt = 0;                                                \
  WORD(SIZE) continues = 0;                                       \
  do {                                                            \
    uint8_t masked  = (uint8_t) (chunk & 0x7f);                   \
    chunk = chunk >> 7;                                           \
    cnt++;                                                        \
    if (chunk) {                                                  \
      *(out++) = masked | 0x80;                                   \
    } else {                                                      \
      *(out++) = masked;                                          \
    }                                                             \
  } while (chunk);                                                \
  return cnt;                                                     \
}

#define PUT_VARINT_IMPL(SIZE)                                     \
uint8_t put_var_int##SIZE##_t (WORD(SIZE) chunk, uint8_t* out) {  \
  return put_var_uint##SIZE##_t(zigzag##SIZE(chunk), out);        \
}

ZIGZAG(8)
ZIGZAG(16)
ZIGZAG(32)
ZIGZAG(64)

PUT_VARWORD_IMPL(8)
PUT_VARWORD_IMPL(16)
PUT_VARWORD_IMPL(32)
PUT_VARWORD_IMPL(64)

PUT_VARINT_IMPL(8)
PUT_VARINT_IMPL(16)
PUT_VARINT_IMPL(32)
PUT_VARINT_IMPL(64)


/* ORC v0 files use Run Length Encoding version 1 (RLEv1), which
 * provides a lightweight compression of signed or unsigned integer
 * sequences. RLEv1 has two sub-encodings:
 *
 *     Run - a sequence of values that differ by a small fixed delta
 *     Literals - a sequence of varint encoded values
 *
 * Runs start with an initial byte of 0x00 to 0x7f, which encodes the
 * length of the run - 3. A second byte provides the fixed delta in
 * the range of -128 to 127. Finally, the first value of the run is
 * encoded as a base 128 varint.
 *
 * For example, if the sequence is 100 instances of 7 the encoding
 * would start with 100 - 3, followed by a delta of 0, and a varint
 * of 7 for an encoding of [0x61, 0x00, 0x07]. To encode the sequence
 * of numbers running from 100 to 1, the first byte is 100 - 3, the
 * delta is -1, and the varint is 100 for an encoding of [0x61, 0xff,
 * 0x64].
 *
 * Literals start with an initial byte of 0x80 to 0xff, which corresponds
 * to the negative of number of literals in the sequence. Following
 * the header byte, the list of N varints is encoded. Thus, if there
 * are no runs, the overhead is 1 byte for each 128 integers. The first
 * 5 prime numbers [2, 3, 4, 7, 11] would encoded as [0xfb, 0x02, 0x03,
 * 0x04, 0x07, 0xb].
 */

#define RLE_INT(SIZE)                                                                     \
uint64_t write_int##SIZE##_rle (const uint64_t length, const INT(SIZE) * input, uint8_t* output) { \
  uint64_t written   = 0;                                                                 \
  uint8_t* writehead = output;                                                            \
  uint8_t cb[3];                                                                          \
                                                                                          \
  while (written < length) {                                                              \
    uint8_t*  headerpos    = writehead++;                                                 \
    INT(SIZE) runvalue     = *(input++);                                                  \
    uint8_t   litsize      = 1;                                                           \
    uint16_t  runsize      = 0;                                                           \
    INT(SIZE) lastmatches  = 0;                                                           \
    INT(SIZE) towrite      = 0;                                                           \
    uint8_t   writesize    = put_var_int##SIZE##_t (runvalue, writehead);                   \
    writehead += writesize;                                                               \
                                                                                          \
    cb[0] = writesize;                                                                    \
                                                                                          \
    while (litsize < 128 && litsize + written < length) {                                 \
      towrite = *(input++);                                                               \
      writesize = put_var_int##SIZE##_t (towrite, writehead);                               \
      writehead += writesize;                                                             \
      cb[litsize % 3] = writesize;                                                        \
      litsize++;                                                                          \
                                                                                          \
      if (litsize >= 3 && lastmatches <= 127 && lastmatches >= -128 && lastmatches == towrite - runvalue) { \
        runsize    = 3;                                                                   \
        litsize   -= runsize;                                                             \
        writehead -= cb[0] + cb[1] + cb[2];                                               \
        runvalue  -= lastmatches;                                                         \
        break;                                                                            \
      }                                                                                   \
                                                                                          \
      lastmatches = towrite - runvalue;                                                   \
      runvalue = towrite;                                                                 \
    }                                                                                     \
                                                                                          \
    if (litsize > 0) {                                                                    \
      *headerpos = ~litsize + 1;                                                          \
      written += litsize;                                                                 \
    } else {                                                                              \
      writehead = headerpos;                                                              \
    }                                                                                     \
                                                                                          \
    if (runsize > 0) {                                                                    \
      while (runsize + written < length && runsize < 130) {                               \
        uint8_t next = *input;                                                            \
        if (next != towrite + lastmatches)                                                \
          break;                                                                          \
                                                                                          \
        towrite = next;                                                                   \
        runsize++;                                                                        \
        input++;                                                                          \
      }                                                                                   \
                                                                                          \
      *(writehead++) = (uint8_t) (runsize - 3);                                           \
      *(writehead++) = (uint8_t) lastmatches;                                             \
      writehead += put_var_int##SIZE##_t (runvalue, writehead);                                   \
      written += runsize;                                                                 \
    }                                                                                     \
  }                                                                                       \
                                                                                          \
  return (writehead - output);                                                            \
}

RLE_INT(8)
RLE_INT(16)
RLE_INT(32)
RLE_INT(64)

#define RLE_UINT(SIZE)                                                                     \
uint64_t write_uint##SIZE##_rle (const uint64_t length, const WORD(SIZE) * input, uint8_t* output) { \
  uint64_t written   = 0;                                                                 \
  uint8_t* writehead = output;                                                            \
  uint8_t cb[3];                                                                          \
                                                                                          \
  while (written < length) {                                                              \
    uint8_t*   headerpos    = writehead++;                                                \
    WORD(SIZE) runvalue     = *(input++);                                                 \
    uint8_t    litsize      = 1;                                                          \
    uint16_t   runsize      = 0;                                                          \
    INT(SIZE)  lastmatches  = 0;                                                          \
    WORD(SIZE) towrite      = 0;                                                          \
    uint8_t    writesize    = put_var_uint##SIZE##_t (runvalue, writehead);                \
    writehead += writesize;                                                               \
                                                                                          \
    cb[0] = writesize;                                                                    \
                                                                                          \
    while (litsize < 128 && litsize + written < length) {                                 \
      towrite = *(input++);                                                               \
      writesize = put_var_uint##SIZE##_t (towrite, writehead);                              \
      writehead += writesize;                                                             \
      cb[litsize % 3] = writesize;                                                        \
      litsize++;                                                                          \
                                                                                          \
      if (litsize >= 3 && lastmatches <= 127 && lastmatches >= -128 && lastmatches == towrite - runvalue) { \
        runsize    = 3;                                                                   \
        litsize   -= runsize;                                                             \
        writehead -= cb[0] + cb[1] + cb[2];                                               \
        runvalue  -= lastmatches;                                                         \
        break;                                                                            \
      }                                                                                   \
                                                                                          \
      lastmatches = towrite - runvalue;                                                   \
      runvalue = towrite;                                                                 \
    }                                                                                     \
                                                                                          \
    if (litsize > 0) {                                                                    \
      *headerpos = ~litsize + 1;                                                          \
      written += litsize;                                                                 \
    } else {                                                                              \
      writehead = headerpos;                                                              \
    }                                                                                     \
                                                                                          \
    if (runsize > 0) {                                                                    \
      while (runsize + written < length && runsize < 130) {                               \
        uint8_t next = *input;                                                            \
        if (next != towrite + lastmatches)                                                \
          break;                                                                          \
                                                                                          \
        towrite = next;                                                                   \
        runsize++;                                                                        \
        input++;                                                                          \
      }                                                                                   \
                                                                                          \
      *(writehead++) = (uint8_t) (runsize - 3);                                           \
      *(writehead++) = (uint8_t) lastmatches;                                             \
      writehead += put_var_uint##SIZE##_t (runvalue, writehead);                          \
      written += runsize;                                                                 \
    }                                                                                     \
  }                                                                                       \
                                                                                          \
  return (writehead - output);                                                            \
}

RLE_UINT(8)
RLE_UINT(16)
RLE_UINT(32)
RLE_UINT(64)
