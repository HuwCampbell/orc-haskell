{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Criterion
import           Criterion.Main

import           Data.Word (Word32)
import qualified Data.Serialize.Put as Put

import qualified Data.Vector.Storable as Storable

import           Orc.Serial.Binary.Internal.Integers


example :: Storable.Vector Word32
example =
  Storable.fromList [1,1,1,3,3,4,5,6,6,6,6,7,7,8,8,8]


main :: IO ()
main =
  defaultMain [
      bgroup "complex" [
        bench "putIntegerRLEv1" $ whnf (Put.runPut . putIntegerRLEv1) example
      ]
    ]
