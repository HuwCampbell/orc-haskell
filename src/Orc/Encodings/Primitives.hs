{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Orc.Encodings.Primitives (
    zigZag64
  , unZigZag64

  , getWordBe
) where

import           Data.Bits (shiftL, shiftR, xor, (.&.), (.|.))
import           Data.Int (Int64)
import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get
import           Data.Word (Word64, Word8)

import           P


zigZag64 :: Int64 -> Word64
zigZag64 !n =
  fromIntegral $! (n `shiftL` 1) `xor` (n `shiftR` 63)
{-# INLINE zigZag64 #-}


unZigZag64 :: Word64 -> Int64
unZigZag64 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
{-# INLINE unZigZag64 #-}


-- | Read a Word64 in big endian format give its size in bits
getWordBe :: Word8 -> Get Word64
getWordBe =
  let
    go :: Word64 -> Word8 -> Get Word64
    go acc n
      | n <= 0
      = return acc
      | otherwise
      = do next <- fromIntegral <$> Get.getWord8
           go ((acc `shiftL` 8) .|. next) (n - 1)
  in
    go 0 . fromIntegral
{-# INLINE getWordBe #-}

