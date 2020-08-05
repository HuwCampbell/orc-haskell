{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Criterion
import           Criterion.Main

import           Data.ByteString (ByteString)
import           Data.Word (Word8, Word32)
import qualified Data.Serialize.Put as Put

import qualified Data.Vector.Storable as Storable

import           Orc.Serial.Binary.Internal.Integers
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

main :: IO ()
main =
  defaultMain [
      bgroup "putIntegerRLEv1" [
        bench "repeats" $ whnf (Put.runPut . putIntegerRLEv1) exampleRepeated
      , bench "complex" $ whnf (Put.runPut . putIntegerRLEv1) exampleComplex
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
