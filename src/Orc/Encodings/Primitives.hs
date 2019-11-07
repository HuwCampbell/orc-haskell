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
  , getWord24le

  , OrcNum (..)
) where

import           Data.Bits (Bits, shiftL, shiftR, xor, (.&.), (.|.))
import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Unsafe as Unsafe
import           Data.Int (Int8, Int16, Int32, Int64)
import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get
import           Data.Word (Word8, Word16, Word32, Word64)
import           Data.WideWord (Int128, Word128)

import           P


-- |
-- Class for either Natural or Integral fixed sized Orc numbers.
--
-- We use these to specify separate Orc reader types in a polymorphic
-- way, with ZigZag encoding being used when appropriate.
--
-- The potentially zigzag encoded natural number type is parameterised
-- with the OrcWord associated type family.
--
class (Bits (OrcWord i), Integral (OrcWord i), Integral i) => OrcNum i where
  type OrcWord i :: *
  zigZag :: i -> OrcWord i
  unZigZag :: OrcWord i -> i


instance OrcNum Word8 where
  type OrcWord Word8 = Word8
  zigZag =
    id
  unZigZag =
    id


instance OrcNum Int8 where
  type OrcWord Int8 = Word8
  zigZag =
    zigZag8
  unZigZag =
    unZigZag8


instance OrcNum Word16 where
  type OrcWord Word16 = Word16
  zigZag =
    id
  unZigZag =
    id


instance OrcNum Int16 where
  type OrcWord Int16 = Word16
  zigZag =
    zigZag16

  unZigZag =
    unZigZag16


instance OrcNum Word32 where
  type OrcWord Word32 = Word32
  zigZag =
    id
  unZigZag =
    id


instance OrcNum Int32 where
  type OrcWord Int32 = Word32
  zigZag =
    zigZag32
  unZigZag =
    unZigZag32


instance OrcNum Word64 where
  type OrcWord Word64 = Word64
  zigZag =
    id
  unZigZag =
    id


instance OrcNum Int64 where
  type OrcWord Int64 = Word64
  zigZag =
    zigZag64

  unZigZag =
    unZigZag64


instance OrcNum Word128 where
  type OrcWord Word128 = Word128
  zigZag =
    id

  unZigZag =
    id


instance OrcNum Int128 where
  type OrcWord Int128 = Word128
  zigZag =
    zigZag128

  unZigZag =
    unZigZag128


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


zigZag128 :: Int128 -> Word128
zigZag128 !n =
  fromIntegral $! (n `shiftL` 1) `xor` (n `shiftR` 127)
{-# INLINE zigZag128 #-}


unZigZag128 :: Word128 -> Int128
unZigZag128 !n =
  fromIntegral $! (n `shiftR` 1) `xor` negate (n .&. 0x1)
{-# INLINE unZigZag128 #-}

-- | Read a Word64 in big endian format given its size in bits
getWordBe :: forall i. OrcNum i => Word8 -> Get i
getWordBe n =
  let
    go :: OrcWord i -> Word8 -> Get (OrcWord i)
    go acc n
      | n <= 0
      = return acc
      | otherwise
      = do next <- fromIntegral <$> Get.getWord8
           go ((acc `shiftL` 8) .|. next) (n - 1)
  in
    unZigZag <$> go 0 n
{-# INLINE getWordBe #-}


getWord24le :: Get Word32
getWord24le = do
  s <- Get.getBytes 3
  return $!
    (fromIntegral (s `Unsafe.unsafeIndex` 2) `shiftL` 16) .|.
    (fromIntegral (s `Unsafe.unsafeIndex` 1) `shiftL`  8) .|.
    (fromIntegral (s `Unsafe.unsafeIndex` 0))
