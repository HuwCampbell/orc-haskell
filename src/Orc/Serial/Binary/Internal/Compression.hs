{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE DoAndIfThenElse     #-}

module Orc.Serial.Binary.Internal.Compression (
    readCompressedStream
) where

import           Control.Exception (tryJust, evaluate)

import qualified Data.Serialize.Get as Get

import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as Lazy

import           Orc.Schema.Types as Orc
import qualified Orc.Serial.Binary.Internal.Get as Get

import           System.IO.Unsafe as Unsafe

import qualified Snapper

import qualified Codec.Compression.Lzo as Lzo
import qualified Codec.Compression.Zlib.Raw as Zlib
import           Codec.Compression.Zlib.Internal (DecompressError)
import qualified Codec.Compression.Zstd as Zstd

import           Orc.Prelude


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
  Just LZO ->
    readLzoParts
  Just LZ4 ->
    const (Left "Unsupported Compression Type LZ4")


overLazy :: (Lazy.ByteString -> Lazy.ByteString) -> ByteString -> ByteString
overLazy f =
  Lazy.toStrict . f . Lazy.fromStrict


readCompressedParts :: (Int -> ByteString -> Either String ByteString) -> ByteString -> Either String ByteString
readCompressedParts action =
  go ByteString.empty
    where
  go acc bytes
    | ByteString.null bytes
    = Right acc
  go acc bytes = do
    header <- Get.runGet Get.getWord24le bytes
    let
      (len, isOriginal) =
        header `divMod` 2

      (pertinent, remaining) =
        ByteString.splitAt (fromIntegral len) $
          ByteString.drop 3 bytes

    thisRound <-
      if isOriginal == 1 then pure pertinent else
        action (fromIntegral len) pertinent

    go (acc <> thisRound) remaining


readSnappyParts :: ByteString -> Either String ByteString
readSnappyParts =
  readCompressedParts $ \_ pertinent ->
    note "Snappy decompression failed." $
      Snapper.decompress pertinent


readZlibParts :: ByteString -> Either String ByteString
readZlibParts =
  readCompressedParts $ \_ pertinent ->
    Unsafe.unsafePerformIO $
      tryJust
        (\(e :: DecompressError) -> Just ("DEFLATE decompression failed with " <> show e))
        (evaluate (overLazy Zlib.decompress pertinent))


readZstdParts :: ByteString -> Either String ByteString
readZstdParts =
  readCompressedParts $ \_ pertinent ->
    case Zstd.decompress pertinent of
      Zstd.Decompress bs
        -> Right bs
      Zstd.Error msg
        -> Left $ "Zstd decompression failed with " <> msg
      Zstd.Skip
        -> Left "Zstd skip encountered"


readLzoParts :: ByteString -> Either String ByteString
readLzoParts =
  readCompressedParts $ \len pertinent ->
    Unsafe.unsafePerformIO $
      tryJust
        (\(e :: DecompressError) -> Just ("DEFLATE decompression failed with " <> show e))
        (evaluate $ Lzo.decompress pertinent len)
