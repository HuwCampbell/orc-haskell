{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Test.Orc.Type where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import qualified Hedgehog.Corpus as Corpus

import           Orc.Prelude

import           Orc.Data.Data
import           Orc.Schema.Types
import           Orc.Serial.Protobuf.Schema


genStructField :: Gen a -> Gen (StructField a)
genStructField genA =
  StructField
    <$> (StructFieldName <$> Gen.element Corpus.weather)
    <*> genA


genType :: Gen Type
genType =
  Gen.recursive Gen.choice
    [ pure BOOLEAN
    , pure BYTE
    , pure SHORT
    , pure INT
    , pure LONG
    , pure FLOAT
    , pure DOUBLE
    , pure STRING
    , pure BINARY
    , pure TIMESTAMP
    , pure DECIMAL
    , pure DATE
    , pure VARCHAR
    , pure CHAR
    ]
    [ LIST   <$> genType
    , MAP    <$> genType <*> genType
    , UNION  <$> Gen.list (Range.linear 1 10) genType
    , STRUCT <$> Gen.list (Range.linear 1 10) (genStructField genType)
    ]


prop_types_roundtrip_protobuf :: Property
prop_types_roundtrip_protobuf =
  property $ do
    typ <- forAll genType
    tripping typ toProtoTypes fromProtoTypes


tests :: IO Bool
tests =
  checkParallel $$(discover)
