{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Orc.Streams.Bytes where

import qualified Data.Serialize.Get as Get
import qualified Data.Serialize.Put as Put
import qualified Data.ByteString as Strict

import qualified Data.Vector.Storable as Storable
import           Data.Word (Word64)

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           P

import           System.IO (IO)

import           Orc.Serial.Encodings.Bytes


prop_roundTripBytes :: Property
prop_roundTripBytes =
  withTests 1000 . property $ do
    x <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.word8 (Range.linearFrom 0 0 maxBound))

    tripping (Storable.fromList x) encodeBytes decodeBytes



tests :: IO Bool
tests =
  checkParallel $$(discover)
