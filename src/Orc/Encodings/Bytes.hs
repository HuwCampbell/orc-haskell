{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ForeignFunctionInterface #-}

module Orc.Encodings.Bytes (
    decodeBytes
  , decodeBits

  -- For Benchmarking
  , decodeBytesNative
) where

import           Data.Bits ((.&.), testBit)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as ByteString
import           Data.Int (Int64)
import           Data.Word (Word8, Word64)
import           Data.Ratio ((%))
import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get
import           Data.String (String)

import qualified Data.Vector.Storable as Storable

import           Foreign (withForeignPtr)
import           Foreign.Ptr (Ptr)

import           System.IO.Unsafe (unsafePerformIO)
import           System.IO as IO

import           P

{-# INLINE decodeBytesNative #-}
decodeBytesNative :: Word64 -> ByteString -> Storable.Vector Word8
decodeBytesNative len bytes =
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
decodeBytes :: ByteString ->  Either String (Storable.Vector Word8)
decodeBytes =
    Get.runGet getBytes


{-# INLINE getBytes #-}
getBytes :: Get (Storable.Vector Word8)
getBytes =
  let
    getSet :: Get (Storable.Vector Word8)
    getSet = do
      header <- Get.getWord8
      if header < 128
        then do
          let
            runLength = header + 3;
          Storable.replicate (fromIntegral runLength) <$> Get.getWord8
        else do
          let
            listLength = header .&. 0x7F;
          Storable.replicateM (fromIntegral listLength) Get.getWord8

  in
    Storable.concat <$>
      many getSet


{-# INLINE decodeBits #-}
decodeBits :: Word64 -> ByteString -> Either String (Storable.Vector Bool)
decodeBits _bitLen bytes =
  let
    -- fitting = ceiling (bitLen % 8)

    decodedByteString =
      decodeBytes bytes

    finiteBits w =
      Storable.map (testBit w) (Storable.enumFromN 0 8)

  in
    fmap (Storable.concatMap finiteBits) decodedByteString
