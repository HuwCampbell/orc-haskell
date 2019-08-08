{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Orc.Encodings.Primitives (
    zigZag64
  , unZigZag64
) where

import           Data.Bits (shiftL, shiftR, xor, (.&.))

import           Data.Int (Int64)
import           Data.Word (Word64)

import           P


zigZag64 :: Int64 -> Word64
zigZag64 !n =
  fromIntegral $! (n `shiftL` 1) `xor` (n `shiftR` 63)
{-# INLINE zigZag64 #-}


unZigZag64 :: Word64 -> Int64
unZigZag64 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
{-# INLINE unZigZag64 #-}
