{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}

module Orc.Serial.Binary.Internal.Put (
    word24LE
) where

import           Data.Bits (shiftR)
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder.Prim as Builder
import qualified Data.ByteString.Builder.Prim.Internal as Builder

import           Data.Word (Word8, Word32)

import           Foreign (plusPtr, poke)

import           Orc.Prelude


-- | Encode a 'Word32' in little endian format.
word24LE :: Word32 -> Builder
word24LE = Builder.primFixed primWord24LE
{-# INLINE word24LE #-}


primWord24LE :: Builder.FixedPrim Word32
primWord24LE = Builder.fixedPrim 3 $ \w p -> do
    poke p               (fromIntegral (w)           :: Word8)
    poke (p `plusPtr` 1) (fromIntegral (shiftR w  8) :: Word8)
    poke (p `plusPtr` 2) (fromIntegral (shiftR w 16) :: Word8)
{-# INLINE primWord24LE #-}
