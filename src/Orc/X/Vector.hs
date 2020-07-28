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

safeHead :: Vector v a => v a -> Maybe a
safeHead v =
  if Vector.null v then
    Nothing
  else
    Just (Vector.unsafeHead v)

uncons :: Vector v a => v a -> Maybe (a, v a)
uncons v =
  if Vector.null v then
    Nothing
  else
    Just (Vector.unsafeHead v, Vector.unsafeTail v)
{-# INLINE uncons #-}
