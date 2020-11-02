{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE FlexibleContexts    #-}

module Orc.Serial.Binary.Internal.Compression (
    readCompressedStream
  , writeCompressedStream
) where

import           Control.Exception (tryJust, evaluate)
import           Control.Monad.Except (MonadError (..), throwError)
import           Control.Monad.IO.Class (MonadIO)
import           Control.Monad.Trans.Class (lift)

import qualified Data.Serialize.Get as Get

import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as Lazy
import qualified Streaming.ByteString as Streaming

import           Orc.Exception.Type
import           Orc.Schema.Types as Orc
import qualified Orc.Serial.Binary.Internal.Get as Get
import qualified Orc.Serial.Binary.Internal.Put as Put
import qualified Orc.Serial.Binary.Internal.Compression.Snappy as Snappy

import           System.IO.Unsafe as Unsafe


import qualified Codec.Compression.Zlib.Raw as Zlib
import           Codec.Compression.Zlib.Internal (DecompressError)
import qualified Codec.Compression.Zstd as Zstd

import           Orc.Prelude
import           Orc.X.Streaming


-- * Reading


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
    const (Left "Unsupported Compression Kind LZO")
  Just LZ4 ->
    const (Left "Unsupported Compression Kind LZ4")


overLazy :: (Lazy.ByteString -> Lazy.ByteString) -> ByteString -> ByteString
overLazy f =
  Lazy.toStrict . f . Lazy.fromStrict


readCompressedParts :: (ByteString -> Either String ByteString) -> ByteString -> Either String ByteString
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
        action pertinent

    go (acc <> thisRound) remaining


readSnappyParts :: ByteString -> Either String ByteString
readSnappyParts =
  readCompressedParts $ \pertinent ->
    note "Snappy decompression failed." $
      Snappy.decompress pertinent


readZlibParts :: ByteString -> Either String ByteString
readZlibParts =
  readCompressedParts $ \pertinent ->
    Unsafe.unsafePerformIO $
      tryJust
        (\(e :: DecompressError) -> Just ("DEFLATE decompression failed with " <> show e))
        (evaluate (overLazy Zlib.decompress pertinent))


readZstdParts :: ByteString -> Either String ByteString
readZstdParts =
  readCompressedParts $ \pertinent ->
    case Zstd.decompress pertinent of
      Zstd.Decompress bs
        -> Right bs
      Zstd.Error msg
        -> Left $ "Zstd decompression failed with " <> msg
      Zstd.Skip
        -> Left "Zstd skip encountered"


-- * Writing


writeCompressedStream
  :: (MonadIO m, MonadError OrcException m)
  => Maybe CompressionKind -> Streaming.ByteStream m r -> Streaming.ByteStream m r
writeCompressedStream = \case
  Nothing ->
    id
  Just NONE ->
    id
  Just SNAPPY ->
    writeSnappyParts
  Just ZLIB ->
    writeZlibParts
  Just ZSTD ->
    writeZstdParts
  Just LZO ->
    const $ lift (throwError (OrcException "Unsupported Compression Kind LZO"))
  Just LZ4 ->
    const $ lift (throwError (OrcException "Unsupported Compression Kind LZ4"))


writeCompressedParts
  :: (MonadIO m, MonadError OrcException m)
  => (ByteString -> ByteString) -> Streaming.ByteStream m r -> Streaming.ByteStream m r
writeCompressedParts action =
  let
    go uncompressed =
      let
        len =
          ByteString.length uncompressed
        compressed =
          action uncompressed
        lenCompressed =
          ByteString.length compressed

      in Streaming.toStreamingByteString $
        if lenCompressed < len then
          let header = 2 * lenCompressed
          in  Put.word24LE (fromIntegral header) <> Builder.byteString compressed
        else
          let header = 2 * len + 1
          in  Put.word24LE (fromIntegral header) <> Builder.byteString uncompressed
  in
    hyloByteStream' go . resizeChunks 262144


writeSnappyParts
  :: (MonadIO m, MonadError OrcException m)
  => Streaming.ByteStream m r -> Streaming.ByteStream m r
writeSnappyParts = writeCompressedParts Snappy.compress

writeZlibParts
  :: (MonadIO m, MonadError OrcException m)
  => Streaming.ByteStream m r -> Streaming.ByteStream m r
writeZlibParts = writeCompressedParts (overLazy Zlib.compress)

writeZstdParts
  :: (MonadIO m, MonadError OrcException m)
  => Streaming.ByteStream m r -> Streaming.ByteStream m r
writeZstdParts = writeCompressedParts (Zstd.compress Zstd.maxCLevel)
