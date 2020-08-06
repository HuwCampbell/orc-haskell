{-# LANGUAGE BangPatterns             #-}
{-# LANGUAGE CPP                      #-}
{-# LANGUAGE DoAndIfThenElse          #-}
{-# LANGUAGE NoImplicitPrelude        #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE LambdaCase               #-}
{-# LANGUAGE PatternGuards            #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE TypeApplications         #-}

module Orc.Serial.Binary.Internal.Integers.Native (
    PutIntegerRLEv1
  , encodeIntegerRLEv1
) where

import           Data.Bits (finiteBitSize)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as ByteString

import           Data.Word (Word8, Word16, Word32)

import qualified Data.Vector.Storable as Storable

import           Foreign (withForeignPtr)
import           Foreign.Ptr (Ptr)

import           Orc.Serial.Binary.Internal.OrcNum

import           System.IO
import           System.IO.Unsafe (unsafePerformIO)

import           Orc.Prelude
import qualified Prelude as Savage

foreign import ccall unsafe
  write_uint8_rle
    :: Int64 -> Ptr Word8 -> Ptr Word8 -> IO Int64

foreign import ccall unsafe
  write_uint16_rle
    :: Int64 -> Ptr Word16 -> Ptr Word8 -> IO Int64

foreign import ccall unsafe
  write_uint32_rle
    :: Int64 -> Ptr Word32 -> Ptr Word8 -> IO Int64

foreign import ccall unsafe
  write_uint64_rle
    :: Int64 -> Ptr Word64 -> Ptr Word8 -> IO Int64

foreign import ccall unsafe
  write_int8_rle
    :: Int64 -> Ptr Int8 -> Ptr Word8 -> IO Int64

foreign import ccall unsafe
  write_int16_rle
    :: Int64 -> Ptr Int16 -> Ptr Word8 -> IO Int64

foreign import ccall unsafe
  write_int32_rle
    :: Int64 -> Ptr Int32 -> Ptr Word8 -> IO Int64

foreign import ccall unsafe
  write_int64_rle
    :: Int64 -> Ptr Int64 -> Ptr Word8 -> IO Int64

class OrcNum w => PutIntegerRLEv1 w where
  write_native :: Int64 -> Ptr w -> Ptr Word8 -> IO Int64

instance PutIntegerRLEv1 Word8 where
  write_native = write_uint8_rle

instance PutIntegerRLEv1 Int8 where
  write_native = write_int8_rle

instance PutIntegerRLEv1 Word16 where
  write_native = write_uint16_rle

instance PutIntegerRLEv1 Int16 where
  write_native = write_int16_rle

instance PutIntegerRLEv1 Word32 where
  write_native = write_uint32_rle

instance PutIntegerRLEv1 Int32 where
  write_native = write_int32_rle

instance PutIntegerRLEv1 Word64 where
  write_native = write_uint64_rle

instance PutIntegerRLEv1 Int64 where
  write_native = write_int64_rle

encodeIntegerRLEv1 :: forall w. PutIntegerRLEv1 w => Storable.Vector w -> ByteString
encodeIntegerRLEv1 bytes =
  unsafePerformIO $ do
    let
      (inPtr, _inLen) =
        Storable.unsafeToForeignPtr0 bytes
      len =
        Storable.length bytes
      fsize
        = finiteBitSize (Savage.undefined :: (OrcWord w)) `div` 7 + 1
      maxSize =
        fsize * len + len `div` 128 + 1

    outPtr <-
      ByteString.mallocByteString maxSize

    reLen <- withForeignPtr inPtr $ \inPtr' ->
      withForeignPtr outPtr $ \outPtr' ->
        write_native (fromIntegral len) inPtr' outPtr'

    return $
      ByteString.fromForeignPtr outPtr 0 (fromIntegral reLen)

