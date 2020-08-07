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

module Orc.Serial.Binary.Internal.Floats (
    decodeFloat32
  , decodeFloat64
  , putFloat32
  , putFloat64

  , decodeFloat32_reference
  , decodeFloat64_reference
) where

import qualified Data.Serialize.Get as Get
import           Data.Serialize.Put (Putter)

import qualified Data.Serialize.IEEE754 as IEEE754

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Internal as ByteString
import           Data.Word (Word8, Word32)
import           Data.String (String)

import qualified Data.Vector.Storable as Storable

import           Foreign (withForeignPtr, plusPtr)
import           Foreign.Ptr (Ptr)

import           System.IO
import           System.IO.Unsafe (unsafePerformIO)

import           Orc.Prelude


decodeFloat32 :: Int -> ByteString -> Either String (Storable.Vector Float)
decodeFloat32 len bytes = do
  let
    bSize = len * 4

  unless (ByteString.length bytes == bSize) $
    Left $ "Float array is incorrectly sized. Expected: " <> show bSize <> "; got: " <> show (ByteString.length bytes)

  unsafePerformIO $ do
    let
      (inPtr, offset, _inLen) =
        ByteString.toForeignPtr bytes

    outPtr <-
      ByteString.mallocByteString bSize

    withForeignPtr inPtr $ \inPtr' ->
      withForeignPtr outPtr $ \outPtr' ->
        read_le_uint32 (fromIntegral len) (inPtr' `plusPtr` offset) outPtr'

    return . Right $
      Storable.unsafeCast $
        Storable.unsafeFromForeignPtr outPtr 0 len


decodeFloat64 :: Int -> ByteString -> Either String (Storable.Vector Double)
decodeFloat64 len bytes = do
  let
    bSize = len * 8

  unless (ByteString.length bytes == bSize) $
    Left $ "Float array is incorrectly sized. Expected: " <> show bSize <> "; got: " <> show (ByteString.length bytes)


  unsafePerformIO $ do
    let
      (inPtr, offset, _inLen) =
        ByteString.toForeignPtr bytes

    outPtr <-
      ByteString.mallocByteString bSize

    withForeignPtr inPtr $ \inPtr' ->
      withForeignPtr outPtr $ \outPtr' ->
        read_le_uint64 (fromIntegral len) (inPtr' `plusPtr` offset) outPtr'

    return . Right $
      Storable.unsafeCast $
        Storable.unsafeFromForeignPtr outPtr 0 len


decodeFloat32_reference :: Int -> ByteString -> Either String (Storable.Vector Float)
decodeFloat32_reference len bytes =
  Get.runGet (Storable.replicateM len IEEE754.getFloat32le) bytes


decodeFloat64_reference :: Int -> ByteString -> Either String (Storable.Vector Double)
decodeFloat64_reference len bytes =
  Get.runGet (Storable.replicateM len IEEE754.getFloat64le) bytes


putFloat32 :: Putter (Storable.Vector Float)
putFloat32 vecs =
  Storable.forM_ vecs IEEE754.putFloat32le


putFloat64 :: Putter (Storable.Vector Double)
putFloat64 vecs =
  Storable.forM_ vecs IEEE754.putFloat64le


foreign import ccall unsafe
  read_le_uint32
    :: Int64 -> Ptr Word8 -> Ptr Word32 -> IO ()

foreign import ccall unsafe
  read_le_uint64
    :: Int64 -> Ptr Word8 -> Ptr Word64 -> IO ()
