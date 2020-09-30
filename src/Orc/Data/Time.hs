{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE DoAndIfThenElse     #-}

module Orc.Data.Time (
    Day (..)
  , Date (..)
  , dayToDate
  , dateToDay

  , Timestamp (..)
  , DateTime (..)
  , timestampToDateTime
  , dateTimeToTimestamp
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

-- | Convert Orc days to simple Date type
--
-- >>> dayToDate $ Day (0)
-- Date {dateYear = 1970, dateMonth = 1, dateDay = 1}

-- >>> dayToDate $ Day (-133255)
-- Date {dateYear = 1605, dateMonth = 2, dateDay = 28}
dayToDate :: Day -> Date
dayToDate (Day g) =
  let
    -- Number of days since 0000-03-01
    oe = g + 719468
    y = ((10000*oe + 14780) `div` 3652425)
    ddd = oe - (365*y + y`div`4 - y`div`100 + y`div`400)

    (yyy, ddx) =
      if (ddd < 0) then
        let
          yy = y - 1
        in
          (yy, oe - (365*yy + yy`div`4 - yy`div`100 + yy`div`400))
      else
        (y, ddd)

    mi = (100 * ddx + 52) `div` 3060
    mm = (mi + 2) `mod` 12 + 1

    dd = ddx - (mi*306 + 5)`div`10 + 1
  in
    Date (yyy + (mi + 2)`div`12) mm dd

-- >>> timestampToDateTime $ Timestamp 0 0
-- DateTime {dtDate = Date {dateYear = 2015, dateMonth = 1, dateDay = 1}, dtTime = 0}
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


-- >>> dateToDay $ Date {dateYear = 1970, dateMonth = 1, dateDay = 1}
-- Day {getDay = 0}
dateToDay :: Date -> Day
dateToDay (Date y0 m0 d) =
  let
    !m =
      (m0 + 9) `rem` 12

    !y =
      y0 - m `quot` 10

    !days =
      365 * y +
      y `quot` 4 -
      y `quot` 100 +
      y `quot` 400 +
      (m * 306 + 5) `quot` 10 +
      (d - 1)

    !date =
       Day (days - 719468)
 in
   date
{-# INLINABLE dateToDay #-}


-- >>> dateTimeToTimestamp $ DateTime {dtDate = Date {dateYear = 2015, dateMonth = 1, dateDay = 1}, dtTime = 0}
-- Timestamp {timestampSeconds = 0, timestampNanos = 0}
dateTimeToTimestamp :: DateTime -> Timestamp
dateTimeToTimestamp (DateTime date tod) = do
  let
    Day !days =
      dateToDay date

    !epoch =
      days - 16436

    !epochSeconds =
      epoch * 86400

    !(daySeconds, nanos) =
      fromIntegral tod `divMod` 1000000000

    !time =
      Timestamp (daySeconds + epochSeconds) (fromIntegral nanos)

  time
{-# INLINABLE dateTimeToTimestamp #-}
