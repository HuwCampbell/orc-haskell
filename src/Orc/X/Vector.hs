{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}

module Orc.X.Vector (
    uncons
  , safeHead
) where

import           Orc.Prelude

import           Data.Vector.Generic (Vector)
import qualified Data.Vector.Generic as Vector

-- | Take the first entry
-- >>> safeHead (Data.Vector.fromList [1, 2])
-- Just 1
safeHead :: Vector v a => v a -> Maybe a
safeHead v =
  if Vector.null v then
    Nothing
  else
    Just (Vector.unsafeHead v)


-- | Pop the first entry from a Vector
-- >>> uncons (Data.Vector.fromList [1, 2])
-- Just (1,[2])
uncons :: Vector v a => v a -> Maybe (a, v a)
uncons v =
  if Vector.null v then
    Nothing
  else
    Just (Vector.unsafeHead v, Vector.unsafeTail v)
{-# INLINE uncons #-}
