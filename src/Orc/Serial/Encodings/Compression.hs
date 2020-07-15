{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE DoAndIfThenElse     #-}

module Orc.Serial.Encodings.Compression (
    readCompressedStream
) where

import           Control.Exception (tryJust, evaluate)

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

import qualified Codec.Compression.Zstd as Zstd

import           P


readCompressedStream :: Maybe CompressionKind -> ByteString -> Either String ByteString
readCompressedStream = \case
  Nothing ->
    Right
  Just NONE ->
    Right
  Just SNAPPY ->
    readSnappyParts
  Just ZLIB ->
    readZlibParts
  Just ZSTD ->
    readZstdParts
  Just u ->
    const (Left $ "Unsupported Compression Kind: " <> show u)


note :: a -> Maybe b -> Either a b
note x = maybe (Left x) Right


overLazy :: (Lazy.ByteString -> Lazy.ByteString) -> ByteString -> ByteString
overLazy f =
  Lazy.toStrict . f . Lazy.fromStrict


readSnappyParts :: ByteString -> Either String ByteString
readSnappyParts bytes
  | ByteString.null bytes
  = Right ByteString.empty
readSnappyParts bytes = do
  header <- Get.runGet Get.getWord24le bytes
  let
    (len, isOriginal) =
      header `divMod` 2

    (pertinent, remaining) =
      ByteString.splitAt (fromIntegral len) $
        ByteString.drop 3 bytes

  thisRound <-
    if isOriginal == 1 then pure pertinent else
      note "Snappy decompression failed." $
        Snapper.decompress pertinent

  fmap (thisRound <>) $
    readSnappyParts remaining


readZlibParts :: ByteString -> Either String ByteString
readZlibParts bytes
  | ByteString.null bytes
  = Right ByteString.empty
readZlibParts bytes = do
  header <- Get.runGet Get.getWord24le bytes
  let
    (len, isOriginal) =
      header `divMod` 2

    (pertinent, remaining) =
      ByteString.splitAt (fromIntegral len) $
        ByteString.drop 3 bytes

  thisRound <-
    if isOriginal == 1 then pure pertinent else
      Unsafe.unsafePerformIO $
        tryJust
          (\(e :: DecompressError) -> Just ("DEFLATE decompression failed with " <> show e))
          (evaluate (overLazy Zlib.decompress pertinent))

  fmap (thisRound <>) $
    readZlibParts remaining



readZstdParts :: ByteString -> Either String ByteString
readZstdParts bytes
  | ByteString.null bytes
  = Right ByteString.empty
readZstdParts bytes = do
  header <- Get.runGet Get.getWord24le bytes
  let
    (len, isOriginal) =
      header `divMod` 2

    (pertinent, remaining) =
      ByteString.splitAt (fromIntegral len) $
        ByteString.drop 3 bytes

  thisRound <-
    if isOriginal == 1 then pure pertinent else
      case Zstd.decompress pertinent of
        Zstd.Decompress bs
          -> Right bs
        Zstd.Error msg
          -> Left $ "Zstd decompression failed with " <> msg
        Zstd.Skip
          -> Left "Zstd skip encountered"

  fmap (thisRound <>) $
    readZstdParts remaining

