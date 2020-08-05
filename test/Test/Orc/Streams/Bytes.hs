{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Orc.Streams.Bytes where

import qualified Data.Vector.Storable as Storable

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           Orc.Prelude
import           Prelude (maxBound)

import           Orc.Serial.Binary.Internal.Bytes


prop_roundtrip_bytes :: Property
prop_roundtrip_bytes =
  withTests 1000 . property $ do
    x <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.word8 (Range.linearFrom 0 0 maxBound))

    tripping (Storable.fromList x) encodeBytes decodeBytes


prop_roundtrip_bytes_native :: Property
prop_roundtrip_bytes_native =
  withTests 1000 . property $ do
    x <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.word8 (Range.linearFrom 0 0 maxBound))

    tripping (Storable.fromList x) encodeBytes (Just . decodeBytesNative (fromIntegral $ length x))



prop_roundtrip_boolean_bits :: Property
prop_roundtrip_boolean_bits =
  withTests 1000 . property $ do
    x <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.bool)

    tripping
      (Storable.fromList x)
      encodeBits
      (fmap (Storable.take (length x)) . decodeBits)



tests :: IO Bool
tests =
  checkParallel $$(discover)
