{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module Orc.Encodings.Bytes (
    decodeBytes
  , decodeBits
) where

-- import           Data.Bits ((.&.), (.|.), complement, shiftL, shiftR, bit, countLeadingZeros, xor)

import           Data.Bits ((.&.), finiteBitSize, testBit)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Internal as ByteString
import           Data.Coerce (coerce)
import           Data.Int (Int8, Int64)
import           Data.Word (Word8, Word64)
import           Data.Ratio ((%))
import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get
import           Data.String (String)

import qualified Data.Vector.Storable as Storable
import qualified X.Data.Vector.Grow as Growable

import           Foreign (mallocForeignPtrArray, withForeignPtr)
import           Foreign.Ptr (Ptr)

import           System.IO.Unsafe (unsafePerformIO)
import           System.IO as IO

import           P

{-# INLINE decodeBytes_native #-}
decodeBytes_native :: Word64 -> ByteString -> Storable.Vector Word8
decodeBytes_native len bytes =
  unsafePerformIO $ do
    let
      (inPtr, offset, _inLen) =
        ByteString.toForeignPtr bytes

    outPtr <- ByteString.mallocByteString (fromIntegral len)

    withForeignPtr inPtr $ \inPtr' ->
      withForeignPtr outPtr $ \outPtr' ->
        read_bytes_rle (fromIntegral len) inPtr' outPtr'

    return $
      Storable.unsafeFromForeignPtr outPtr 0 (fromIntegral len)

foreign import ccall unsafe
  read_bytes_rle
    :: Int64 -> Ptr Word8 -> Ptr Word8 -> IO ()


{-# INLINE decodeBytes #-}
decodeBytes :: Word64 -> ByteString ->  Either String (Storable.Vector Word8)
decodeBytes len bytes =
    Get.runGet (getBytes len) bytes


{-# INLINE getBytes #-}
getBytes :: Word64 -> Get (Storable.Vector Word8)
getBytes len =
  let
    getSet :: Get (Storable.Vector Word8)
    getSet = do
      header <- Get.getWord8
      if header < 128
        then do
          let
            runLength = header + 3;
          value <- Get.getWord8

          return $
            Storable.replicate (fromIntegral runLength) value
        else do
          let
            listLength = header .&. 0x7F;
          Storable.replicateM (fromIntegral listLength) Get.getWord8

  in
    Storable.concat <$>
      many getSet


{-# INLINE decodeBits #-}
decodeBits :: Word64 -> ByteString -> Either String (Storable.Vector Bool)
decodeBits bitLen bytes =
  let
    fitting = ceiling (bitLen % 8)

    decodedByteString =
      decodeBytes fitting bytes

    finiteBits w =
      Storable.map (testBit w) (Storable.enumFromN 0 8)

  in
    fmap (Storable.concatMap finiteBits) decodedByteString
