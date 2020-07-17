{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}

module Orc.X.Vector (
    xmapM
  , ximapM
  , xmapMaybe
  , uncons
  , safeHead
) where

import           Orc.Prelude

import           Data.Vector.Generic (Vector, Mutable)
import qualified Data.Vector.Generic as Vector
import qualified Data.Vector.Generic.Mutable as MVector
import           Data.Vector.Generic.Mutable (MVector)

import           Control.Monad.ST


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


xmapMaybe :: (Vector v a, Vector v b, MVector (Mutable v) b) => x -> (a -> Maybe b) -> v a -> Either x (v b)
xmapMaybe reason f xs = {-# SCC xmapMaybe #-}
  ximapM (\_ x -> note reason (f x)) xs
{-# INLINE xmapMaybe #-}


xmapM :: (Vector v a, Vector v b, MVector (Mutable v) b) => (a -> Either x b) -> v a -> Either x (v b)
xmapM f xs = {-# SCC xmapM #-}
  ximapM (\_ x -> f x) xs
{-# INLINE xmapM #-}


ximapM :: (Vector v a, Vector v b, MVector (Mutable v) b) => (Int -> a -> Either x b) -> v a -> Either x (v b)
ximapM f xs = {-# SCC ximapM #-}
  runST $ do
    let
      !n =
        Vector.length xs

    m <- MVector.new n

    let
      loop !i =
        if i < n then
          let
            !x0 =
              Vector.unsafeIndex xs i
          in
            case f i x0 of
              Left err ->
                pure $! Left err
              Right x -> do
                MVector.unsafeWrite m i x
                loop (i + 1)
        else do
          !ys <- Vector.unsafeFreeze m
          pure $! Right ys

    loop 0
{-# INLINE ximapM #-}
