{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}

module Orc.Serial.Binary.Internal.Get (
    getWord24le
  , getOrcNumBe
  , getOrcNumBePatchedBase
) where

import           Data.Bits (shiftL, (.|.), testBit, clearBit)
import qualified Data.ByteString.Unsafe as Unsafe
import qualified Data.ByteString.Internal as Internal
import           Data.Word (Word8, Word32)
import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get

import           Orc.Serial.Binary.Internal.OrcNum

import           Orc.Prelude


getWord24le :: Get Word32
getWord24le = do
  s <- Get.getBytes 3
  return $!
    (fromIntegral (s `Unsafe.unsafeIndex` 2) `shiftL` 16) .|.
    (fromIntegral (s `Unsafe.unsafeIndex` 1) `shiftL`  8) .|.
    (fromIntegral (s `Unsafe.unsafeIndex` 0))
{-# INLINE getWord24le #-}

-- | Read an Orc Num in big endian format given its size in bits
getOrcNumBe :: forall i. OrcNum i => Word8 -> Get i
getOrcNumBe n =
  let
    go :: OrcWord i -> Word8 -> Get (OrcWord i)
    go acc m
      | m <= 0
      = return acc
      | otherwise
      = do next <- fromIntegral <$> Get.getWord8
           go ((acc `shiftL` 8) .|. next) (m - 1)
  in
    unZigZag <$> go 0 n
{-# INLINE getOrcNumBe #-}


-- | Read an Orc Num for patched base
--
--   Base value (BW bytes) - The base value is stored as a big
--   endian value with negative values marked by the most
--   significant bit set. If it that bit is set, the entire
--   value is negated.
--
--   Don't know why they didn't just zigzag like Short Repeat.
getOrcNumBePatchedBase :: forall i. OrcNum i => Word8 -> Get i
getOrcNumBePatchedBase n = do
  allBytes <- Get.getBytes (fromIntegral n)
  b:bs     <- pure $ Internal.unpackBytes allBytes

  let
    signIndicator =
      testBit b 7

    cleared =
      if signIndicator then
        Internal.packBytes (clearBit b 7 : bs)
      else
        allBytes

    fromOrcWord :: OrcWord i -> i
    fromOrcWord =
      if signIndicator then
        negate . fromIntegral
      else
        fromIntegral

    go :: OrcWord i -> Word8 -> Get (OrcWord i)
    go acc m
      | m <= 0
      = return acc
      | otherwise
      = do next <- fromIntegral <$> Get.getWord8
           go ((acc `shiftL` 8) .|. next) (m - 1)

  either fail (pure . fromOrcWord) $
    Get.runGet (go 0 n) cleared
{-# INLINE getOrcNumBePatchedBase #-}
