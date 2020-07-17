{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE DoAndIfThenElse     #-}

module Orc.Data.Time (
    Day (..)
  , Date (..)
  , dayToDate

  , Timestamp (..)
  , DateTime (..)
  , timestampToDateTime
) where

import Orc.Prelude

-- Number of days after January 1, 1970
newtype Day = Day {
    getDay :: Int64
  } deriving (Eq, Ord, Show)

data Timestamp = Timestamp {
    -- | Number of seconds since January 1, 2015
    timestampSeconds  :: !Int64
    -- | Number of nanoseconds
  , timestampNanos    :: !Word64
  } deriving (Eq, Ord, Show)


data Date = Date {
    dateYear  :: !Int64
  , dateMonth :: !Int64
  , dateDay   :: !Int64
  } deriving (Eq, Ord, Show)


data DateTime = DateTime {
    dtDate :: !Date
  , dtTime :: !Word64
  } deriving (Eq, Ord, Show)


dayToDate :: Day -> Date
dayToDate (Day g) =
  let
    -- Number of days since 0000-03-01
    oe = g + 719468
    y = ((10000*oe + 14780) `div` 3652425)
    ddd = oe - (365*y + y`div`4 - y`div`100 + y`div`400)

    ddx =
      if (ddd < 0) then
        let
          yy = y - 1
        in
          g - (365*y + yy `div` 4 - yy `div` 100 + yy `div` 400)
      else
        ddd

    mi = (100 * ddx + 52) `div` 3060
    mm = (mi + 2) `mod` 12 + 1

    dd = ddx - (mi*306 + 5)`div`10 + 1
  in
    Date (y + (mi + 2)`div`12) mm dd


timestampToDateTime :: Timestamp -> DateTime
timestampToDateTime (Timestamp ts tn) =
  let
    (days, sid) =
      ts `divMod` 86400
    epoch =
      days + 16436
    date =
      dayToDate $ Day epoch
    nanos =
      fromIntegral sid * 10^(9 :: Int) + tn
  in
    DateTime date nanos
