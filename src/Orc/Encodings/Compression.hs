{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}

module Orc.Encodings.Compression (
    readCompressedStream
) where

import           Control.Exception (SomeException, tryJust, evaluate)
import           Control.Monad.Trans.Either (EitherT, newEitherT, left)

import qualified Data.Serialize.Get as Get

import           Data.Word (Word64)
import           Data.WideWord (Int128, Word128)

import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable


import           Viking (Of (..), Stream)
import qualified Viking.Stream as Viking

import           Orc.Data.Segmented
import           Orc.Data.Data (StructField (..), Indexed, currentIndex, currentValue, nextIndex, makeIndexed)
import           Orc.Data.Striped (Column (..))
import           Orc.Schema.Types as Orc
import           Orc.Encodings.Primitives

import           System.IO as IO
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
      header <- Get.runGet getWord24le bytes
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
      header <- Get.runGet getWord24le bytes
      let
        (len, isOriginal) =
          header `divMod` 2

        pertinent =
          ByteString.take (fromIntegral len) $
            ByteString.drop 3 bytes

      if isOriginal == 1 then pure pertinent else
        Unsafe.unsafePerformIO $
          tryJust
            (\(_ :: DecompressError) -> Just ("DEFLATE decompression failed"))
            (evaluate (overLazy Zlib.decompress pertinent))

  Just u ->
    const (Left $ "Unsupported Compression Kind: " <> show u)


note :: a -> Maybe b -> Either a b
note x = maybe (Left x) Right


overLazy :: (Lazy.ByteString -> Lazy.ByteString) -> ByteString -> ByteString
overLazy f =
  Lazy.toStrict . f . Lazy.fromStrict
