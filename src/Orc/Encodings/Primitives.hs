{-# LANGUAGE BangPatterns           #-}
{-# LANGUAGE NoImplicitPrelude      #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE FlexibleContexts       #-}
module Orc.Encodings.Primitives (
    zigZag64
  , unZigZag64

  , getWordBe
  , OrcNum (..)
) where

import           Data.Bits (Bits, shiftL, shiftR, xor, (.&.), (.|.))
import           Data.Int (Int8, Int16, Int32, Int64)
import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get
import           Data.Word (Word8, Word16, Word32, Word64)

import           P


class (Bits (OrcWord i), Integral (OrcWord i), Integral i) => OrcNum i where
  type OrcWord i :: *
  zigZag :: i -> OrcWord i
  unZigZag :: OrcWord i -> i


instance OrcNum Word8 where
  type OrcWord Word8 = Word8
  zigZag = id
  unZigZag = id


instance OrcNum Int8 where
  type OrcWord Int8 = Word8
  zigZag =
    zigZag8
  unZigZag =
    unZigZag8

instance OrcNum Word16 where
  type OrcWord Word16 = Word16
  zigZag = id
  unZigZag = id


instance OrcNum Int16 where
  type OrcWord Int16 = Word16
  zigZag =
    zigZag16

  unZigZag =
    unZigZag16


instance OrcNum Word32 where
  type OrcWord Word32 = Word32
  zigZag = id
  unZigZag = id


instance OrcNum Int32 where
  type OrcWord Int32 = Word32
  zigZag =
    zigZag32
  unZigZag =
    unZigZag32


instance OrcNum Word64 where
  type OrcWord Word64 = Word64
  zigZag = id
  unZigZag = id


instance OrcNum Int64 where
  type OrcWord Int64 = Word64
  zigZag =
    zigZag64

  unZigZag =
    unZigZag64


zigZag8 :: Int8 -> Word8
zigZag8 !n =
  fromIntegral $! (n `shiftL` 1) `xor` (n `shiftR` 7)
{-# INLINE zigZag8 #-}


unZigZag8 :: Word8 -> Int8
unZigZag8 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
{-# INLINE unZigZag8 #-}


zigZag16 :: Int16 -> Word16
zigZag16 !n =
  fromIntegral $! (n `shiftL` 1) `xor` (n `shiftR` 15)
{-# INLINE zigZag16 #-}


unZigZag16 :: Word16 -> Int16
unZigZag16 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
{-# INLINE unZigZag16 #-}


zigZag32 :: Int32 -> Word32
zigZag32 !n =
  fromIntegral $! (n `shiftL` 1) `xor` (n `shiftR` 31)
{-# INLINE zigZag32 #-}


unZigZag32 :: Word32 -> Int32
unZigZag32 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
{-# INLINE unZigZag32 #-}


zigZag64 :: Int64 -> Word64
zigZag64 !n =
  fromIntegral $! (n `shiftL` 1) `xor` (n `shiftR` 63)
{-# INLINE zigZag64 #-}


unZigZag64 :: Word64 -> Int64
unZigZag64 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
{-# INLINE unZigZag64 #-}


-- | Read a Word64 in big endian format given its size in bits
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
    go 0
{-# INLINE getWordBe #-}
