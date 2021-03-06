{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Orc.Streams.Integer where

import qualified Data.Serialize.Get as Get
import qualified Data.Serialize.Put as Put
import qualified Data.ByteString as Strict

import qualified Data.Vector.Storable as Storable

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           Orc.Prelude
import           Prelude (minBound, maxBound)

import           Orc.Serial.Binary.Internal.Integers
import           Orc.Serial.Binary.Internal.Integers.Native

prop_roundtrip_base128_varint :: Property
prop_roundtrip_base128_varint =
  withTests 1000 . property $ do
    x <-
      forAll $
        Gen.word64 (Range.linearFrom 0 0 maxBound)

    tripping x (Put.runPut . putBase128Varint) (Get.runGet getBase128Varint)


prop_roundtrip_integer_rle_v1 :: Property
prop_roundtrip_integer_rle_v1 =
  withTests 1000 . property $ do
    xs <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.word64 (Range.linearFrom 0 0 maxBound))

    tripping (Storable.fromList xs) encodeIntegerRLEv1 decodeIntegerRLEv1


prop_roundtrip_integer_rle_v1_int64 :: Property
prop_roundtrip_integer_rle_v1_int64 =
  withTests 1000 . property $ do
    xs <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.int64 (Range.linearFrom 0 minBound maxBound))

    tripping (Storable.fromList xs) encodeIntegerRLEv1 decodeIntegerRLEv1


prop_roundtrip_integer_rle_v1_native_int8 :: Property
prop_roundtrip_integer_rle_v1_native_int8 =
  withTests 1000 . property $ do
    xs <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.int8 (Range.linearFrom 0 minBound maxBound))

    tripping (Storable.fromList xs) encodeIntegerRLEv1 decodeIntegerRLEv1

prop_roundtrip_integer_rle_v1_native_int32 :: Property
prop_roundtrip_integer_rle_v1_native_int32 =
  withTests 1000 . property $ do
    xs <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.int32 (Range.linearFrom 0 minBound maxBound))

    tripping (Storable.fromList xs) encodeIntegerRLEv1 decodeIntegerRLEv1

prop_roundtrip_integer_rle_v1_native_int16 :: Property
prop_roundtrip_integer_rle_v1_native_int16 =
  withTests 1000 . property $ do
    xs <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.int16 (Range.linearFrom 0 minBound maxBound))

    tripping (Storable.fromList xs) encodeIntegerRLEv1 decodeIntegerRLEv1

prop_roundtrip_integer_rle_v1_native_int64 :: Property
prop_roundtrip_integer_rle_v1_native_int64 =
  withTests 1000 . property $ do
    xs <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.int64 (Range.linearFrom 0 minBound maxBound))

    tripping (Storable.fromList xs) encodeIntegerRLEv1 decodeIntegerRLEv1


prop_roundtrip_integer_rle_v1_native_word8 :: Property
prop_roundtrip_integer_rle_v1_native_word8 =
  withTests 1000 . property $ do
    xs <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.word8 (Range.linearFrom 0 minBound maxBound))

    tripping (Storable.fromList xs) encodeIntegerRLEv1 decodeIntegerRLEv1


prop_roundtrip_integer_rle_v1_native_word32 :: Property
prop_roundtrip_integer_rle_v1_native_word32 =
  withTests 1000 . property $ do
    xs <-
      forAll $
        Gen.list
          (Range.linear 0 1000)
          (Gen.word32 (Range.linearFrom 0 minBound maxBound))

    tripping (Storable.fromList xs) encodeIntegerRLEv1 decodeIntegerRLEv1


prop_spec_repeat_integer_rle_v1 :: Property
prop_spec_repeat_integer_rle_v1 =
  withTests 1 . property $ do
    let
      shortRepeatExampleInput =
        Strict.pack [0x61, 0x00, 0x07]

      shortRepeatExpected =
        Storable.replicate 100 (7 :: Word64)

      shortRepeatOutput =
        Get.runGet getIntegerRLEv1 shortRepeatExampleInput

    shortRepeatOutput === Right shortRepeatExpected



prop_spec_put_repeat_integer_rle_v1 :: Property
prop_spec_put_repeat_integer_rle_v1 =
  withTests 1 . property $ do
    let
      shortRepeatExampleInput =
        Strict.pack [0x61, 0x00, 0x07]

      shortRepeatExpected =
        Storable.replicate 100 (7 :: Word64)

      shortRepeatOutput =
        Put.runPut (putIntegerRLEv1 shortRepeatExpected)

    shortRepeatOutput === shortRepeatExampleInput


prop_spec_short_repeat_integer_rle_v2 :: Property
prop_spec_short_repeat_integer_rle_v2 =
  withTests 1 . property $ do
    let
      shortRepeatExampleInput =
        Strict.pack [0x0a, 0x27, 0x10]

      shortRepeatExpected :: Storable.Vector Word64
      shortRepeatExpected =
        Storable.fromList [10000, 10000, 10000, 10000, 10000]

      shortRepeatOutput =
        Get.runGet getIntegerRLEv2 shortRepeatExampleInput

    shortRepeatOutput === Right shortRepeatExpected


prop_spec_direct_integer_rle_v2 :: Property
prop_spec_direct_integer_rle_v2 =
  withTests 1 . property $ do
    let
      directExampleInput =
        Strict.pack [0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef]

      directExpected :: Storable.Vector Word64
      directExpected =
        Storable.fromList [23713, 43806, 57005, 48879]

      directOutput =
        Get.runGet getIntegerRLEv2 directExampleInput

    directOutput === Right directExpected



prop_spec_patch_integer_rle_v2 :: Property
prop_spec_patch_integer_rle_v2 =
  withTests 1 . property $ do
    let
      patchExampleInput =
        Strict.pack [0x8e, 0x13, 0x2b, 0x21, 0x07,
          0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0x64, 0x6e,
          0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8]

      patchExpected :: Storable.Vector Word64
      patchExpected =
        Storable.fromList [2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070,
          2080, 2090, 2100, 2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190]

      patchOutput =
        Get.runGet getIntegerRLEv2 patchExampleInput

    patchOutput === Right patchExpected


prop_spec_delta_integer_rle_v2 :: Property
prop_spec_delta_integer_rle_v2 =
  withTests 1 . property $ do
    let
      deltaExampleInput =
        Strict.pack [0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46]

      deltaExpected :: Storable.Vector Word64
      deltaExpected =
        Storable.fromList [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]

      deltaOutput =
        Get.runGet getIntegerRLEv2 deltaExampleInput

    deltaOutput === Right deltaExpected



prop_spec_delta_integer_rle_v2_zero_not_zero :: Property
prop_spec_delta_integer_rle_v2_zero_not_zero =
  withTests 1 . property $ do
    let
      deltaExampleInput =
        Strict.pack [0xc0, 0x09, 0x02, 0x02]

      deltaExpected :: Storable.Vector Word64
      deltaExpected =
        Storable.fromList [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]

      deltaOutput =
        Get.runGet getIntegerRLEv2 deltaExampleInput

    deltaOutput === Right deltaExpected




prop_roundtrip_nanoseconds :: Property
prop_roundtrip_nanoseconds =
  property $ do
    x <-
      forAll $
        Gen.word64 (Range.linearFrom 0 0 1000000000)

    tripping x encodeNanoseconds (Just . decodeNanoseconds)


tests :: IO Bool
tests =
  checkParallel $$(discover)
