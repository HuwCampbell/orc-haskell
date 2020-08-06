{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications  #-}
module Main where

import           Criterion
import           Criterion.Main

import           Data.ByteString (ByteString)
import           Data.Word (Word8, Word32)

import qualified Data.Vector.Storable as Storable

import           Orc.Serial.Binary.Internal.Integers
import           Orc.Serial.Binary.Internal.Integers.Native
import           Orc.Serial.Binary.Internal.Bytes


exampleRepeated :: Storable.Vector Word32
exampleRepeated =
  Storable.replicate 100 1


exampleComplex :: Storable.Vector Word32
exampleComplex =
  Storable.fromList [1,1,1,3,3,4,5,6,6,6,6,7,7,8,8,8]


bytesUnencoded :: Storable.Vector Word8
bytesUnencoded =
  Storable.fromList [1,1,1,3,3,4,5,6,6,6,6,7,7,8,8,8]


bytesComplex :: ByteString
bytesComplex =
  encodeBytes bytesUnencoded


intsComplex :: ByteString
intsComplex =
  encodeIntegerRLEv1 exampleComplex


main :: IO ()
main =
  defaultMain [
      bgroup "putIntegerRLEv1" [
        bench "repeats" $ whnf encodeIntegerRLEv1 exampleRepeated
      , bench "complex" $ whnf encodeIntegerRLEv1 exampleComplex
      , bench "word8"   $ whnf encodeIntegerRLEv1 bytesUnencoded
      ]

    , bgroup "decode ints" [
        bench "haskell" $ whnf (decodeIntegerRLEv1 @ Word32 ) intsComplex
      ]

    , bgroup "decode bytes" [
        bench "haskell" $ whnf decodeBytes bytesComplex
      , bench "native"  $ whnf (decodeBytesNative 16) bytesComplex
      ]

    , bgroup "encode bytes" [
        bench "haskell" $ whnf encodeBytes       bytesUnencoded
      , bench "native"  $ whnf encodeBytesNative bytesUnencoded
      ]
    ]
