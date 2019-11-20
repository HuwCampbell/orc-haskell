{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}

module Orc.Serial.Encodings.Compression (
    readCompressedStream
) where

import           Control.Exception (tryJust, evaluate)

import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get

import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as Lazy

import           Orc.Schema.Types as Orc
import qualified Orc.Serial.Encodings.Get as Get

import           System.IO.Unsafe as Unsafe

import qualified Snapper

import qualified Codec.Compression.Zlib.Raw as Zlib
import           Codec.Compression.Zlib.Internal (DecompressError)

import           P


readCompressedStream :: Maybe CompressionKind -> ByteString -> Either String ByteString
readCompressedStream = \case
  Nothing ->
    Right
  Just NONE ->
    Right
  Just SNAPPY ->
    \bytes -> do
      header <- Get.runGet Get.getWord24le bytes
      let
        (len, isOriginal) =
          header `divMod` 2

        pertinent =
          ByteString.take (fromIntegral len) $
            ByteString.drop 3 bytes

      if isOriginal == 1 then pure pertinent else
        note "Snappy decompression failed." $
          Snapper.decompress pertinent

  Just ZLIB ->
    \bytes -> do
      header <- Get.runGet Get.getWord24le bytes
      let
        (len, isOriginal) =
          header `divMod` 2

        pertinent =
          ByteString.take (fromIntegral len) $
            ByteString.drop 3 bytes

      if isOriginal == 1 then pure pertinent else
        Unsafe.unsafePerformIO $
          tryJust
            (\(e :: DecompressError) -> Just ("DEFLATE decompression failed with " <> show e))
            (evaluate (overLazy Zlib.decompress pertinent))

  Just u ->
    const (Left $ "Unsupported Compression Kind: " <> show u)


note :: a -> Maybe b -> Either a b
note x = maybe (Left x) Right


overLazy :: (Lazy.ByteString -> Lazy.ByteString) -> ByteString -> ByteString
overLazy f =
  Lazy.toStrict . f . Lazy.fromStrict
