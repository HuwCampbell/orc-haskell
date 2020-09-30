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


tests :: IO Bool
tests =
  checkParallel $$(discover)


