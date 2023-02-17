{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Orc.Data.Time where

import           Data.Functor.Identity (Identity (..))
import qualified Data.Map as Map

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           Orc.Prelude

import           Orc.Data.Time
import           Orc.Serial.Binary.Internal.Integers


isLeapYear :: Int64 -> Bool
isLeapYear y =
  (y % 4 == 0 && y % 100 /= 0) || y % 400 == 0

maxDays :: Int64 -> Map.Map Int64 Int64
maxDays y =
  Map.fromList [
    (1, 31)
  , (2, if isLeapYear y then 29 else 28)
  , (3, 31)
  , (4, 30)
  , (5, 31)
  , (6, 30)
  , (7, 31)
  , (8, 31)
  , (9, 30)
  , (10, 31)
  , (11, 30)
  , (12, 31)
  ]

prop_dates_roundtrip :: Property
prop_dates_roundtrip =
  withTests 10000 . property $ do
    year  <- forAll $ Gen.integral (Range.linear 1600 10000)
    month <- forAll $ Gen.integral (Range.linear 1 12)
    day   <- forAll $ Gen.integral (Range.linear 1 (maxDays year Map.! month))
    tripping (Date year month day) dateToDay (Identity . dayToDate)


prop_timestamps_roundtrip :: Property
prop_timestamps_roundtrip =
  withTests 10000 . property $ do
    year  <- forAll $ Gen.integral (Range.linear 1600 10000)
    month <- forAll $ Gen.integral (Range.linear 1 12)
    day   <- forAll $ Gen.integral (Range.linear 1 (maxDays year Map.! month))
    nanos <- forAll $ Gen.integral (Range.linear 0 (86400 * 1^(9::Int) - 1))
    tripping (DateTime (Date year month day) nanos) dateTimeToTimestamp (Identity . timestampToDateTime)


-- Because the number of nanoseconds often has a large number of trailing zeros,
-- the number has trailing decimal zero digits removed and the last three bits
-- are used to record how many zeros were removed. if the trailing zeros are more
-- than 2. Thus 1000 nanoseconds would be serialized as 0x0a and 100000 would be
-- serialized as 0x0c.

prop_timestamps_example_1 :: Property
prop_timestamps_example_1 =
  withTests 1 . property $ do
    let encoded = encodeNanoseconds 1000
        expected = 0x0a
    encoded === expected


prop_timestamps_example_2 :: Property
prop_timestamps_example_2 =
  withTests 1 . property $ do
    let encoded = encodeNanoseconds 100000
        expected = 0x0c
    encoded === expected


tests :: IO Bool
tests =
  checkParallel $$(discover)


