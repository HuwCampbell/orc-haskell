{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Orc.Streaming (tests) where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import qualified Hedgehog.Corpus as Corpus

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Streaming.ByteString as Streaming
import           Streaming.Prelude as Streaming

import           Orc.Prelude

import           Orc.X.Streaming


prop_rechunking :: Property
prop_rechunking =
  property $ do
    bytestrings :: [ByteString] <-
      forAll $
        Gen.list (Range.linear 0 10) $
          Gen.element Corpus.glass

    chunkSize <-
      forAll $
        Gen.int (Range.linear 1 30)

    let
      single =
        ByteString.concat bytestrings

    recreated <-
      evalIO $
        Streaming.toStrict_ $
        Streaming.fromChunks $
        resizeChunks chunkSize $
        Streaming.fromChunks $
          Streaming.each bytestrings

    recreated === single


tests :: IO Bool
tests =
  checkParallel $$(discover)
