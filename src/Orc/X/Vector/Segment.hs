{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Orc.X.Vector.Segment (
    SegmentError (..)

  , reify
  , unsafeReify
  ) where

import qualified Data.Vector as Boxed
import qualified Data.Vector.Generic as Generic

import           P hiding (length, concat, empty)


-- Common with Zebra

data SegmentError =
  SegmentLengthMismatch Int Int
  deriving (Eq, Show)

-- | Reify nested segments in to a vector of segments.
reify :: (Integral i, Generic.Vector v i) => v i -> Boxed.Vector a -> Either SegmentError (Boxed.Vector (Boxed.Vector a))
reify ns xs =
  let
    !n_sum =
      fromIntegral (Generic.sum ns)

    !n_xs =
      Boxed.length xs
  in
    if n_sum /= n_xs then
      Left $ SegmentLengthMismatch n_sum n_xs
    else
      pure $ unsafeReify ns xs
{-# INLINE reify #-}

data IdxOff =
  IdxOff !Int !Int

unsafeReify :: (Integral i, Generic.Vector v i) => v i -> Boxed.Vector a -> Boxed.Vector (Boxed.Vector a)
unsafeReify ns xs =
  let
    loop (IdxOff idx off) =
      let
        !len =
          fromIntegral (Generic.unsafeIndex ns idx)
      in
        Just (Boxed.unsafeSlice off len xs, IdxOff (idx + 1) (off + len))
  in
    Generic.unfoldrN (Generic.length ns) loop (IdxOff 0 0)
{-# INLINE unsafeReify #-}
