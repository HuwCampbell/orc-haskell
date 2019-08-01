{-# LANGUAGE BangPatterns   #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Orc.Encodings.Integers (
    decodeWord64
  , decodeInt64
) where

import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get
import           Data.Bits ((.&.), (.|.), complement, shiftL, shiftR, bit, countLeadingZeros, xor)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Internal as ByteString
import           Data.Coerce (coerce)
import           Data.Int (Int8, Int64)
import           Data.Word (Word8, Word64)
import           Data.String (String)

import qualified Data.Vector.Storable as Storable
import qualified X.Data.Vector.Grow as Growable

import           Foreign (mallocForeignPtrArray, withForeignPtr)
import           Foreign.Ptr (Ptr)

import           System.IO.Unsafe (unsafePerformIO)
import           System.IO as IO



import           P


b10000000 :: Word8
b10000000 = 0x80


b01111111 :: Word8
b01111111 = complement 0x80


terminates :: Word8 -> Bool
terminates firstByte =
  (firstByte .&. b10000000) /= b10000000


{-# INLINE decodeBase128Varint #-}
decodeBase128Varint :: Word64 -> ByteString ->  Either String (Storable.Vector Word64)
decodeBase128Varint len bytes =
  let
    getter =
      Storable.replicateM (fromIntegral len) getBase128Varint
  in
    Get.runGet getter bytes


{-# INLINE getBase128Varint #-}
getBase128Varint :: Get Word64
getBase128Varint =
  let
    go :: Word64 -> Int -> Get Word64
    go !accumulator !shift = do
      byte <- Get.getWord8
      let
        masked =
          byte .&. b01111111

        new =
          (fromIntegral masked `shiftL` shift) .|. accumulator

      if terminates byte
        then return new
        else go new (7 + shift)
  in
    go 0 0


{-# INLINABLE decodeWord64 #-}
decodeWord64 :: Word64 -> ByteString ->  Either String (Storable.Vector Word64)
decodeWord64 = decodeBase128Varint



{-# INLINABLE decodeInt64 #-}
decodeInt64 :: Word64 -> ByteString -> Either String (Storable.Vector Int64)
decodeInt64 len bytes =
  let
    words = decodeWord64 len bytes
  in
    fmap (Storable.map unZigZag64) words



zigZag64 :: Int64 -> Word64
zigZag64 !n =
  fromIntegral $! (n `shiftL` 1) `xor` (n `shiftR` 63)
{-# INLINE zigZag64 #-}


unZigZag64 :: Word64 -> Int64
unZigZag64 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
{-# INLINE unZigZag64 #-}
