{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE LambdaCase #-}
module Test.Orc.Logical (
  tests
) where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import qualified Hedgehog.Corpus as Corpus

import           Control.Monad.IO.Class
import           Control.Monad.Morph
import           Control.Monad.Trans.Resource (runResourceT)

import qualified Data.List as List
import           Data.Vector

import           Orc.Prelude
import           Prelude (minBound, maxBound, Bounded)

import qualified Orc.Data.Time as Time
import           Orc.Schema.Types
import           Orc.Table.Logical
import qualified Orc.Table.Convert as Convert

import qualified Orc.Serial.Binary.Logical as Binary

import           Test.Orc.Type (genType)

import           Streaming (Of (..))
import qualified Streaming.Prelude as Streaming

import qualified System.IO.Temp as Temp
import           System.FilePath ((</>))

lmb :: (Integral a, Bounded a) => Range a
lmb = Range.linearFrom 0 minBound maxBound


genLogical :: Type -> Gen Row
genLogical = \case
  BOOLEAN ->
    Bool <$> Gen.bool
  BYTE ->
    Bytes <$> Gen.word8 lmb
  SHORT ->
    Short <$> Gen.int16 lmb
  INT ->
    Integer <$> Gen.int32 lmb
  LONG ->
    Long <$> Gen.int64 lmb
  FLOAT ->
    Float <$> Gen.float (Range.exponentialFloatFrom 0 (-1e6) (1e6))
  DOUBLE ->
    Double <$> Gen.double (Range.exponentialFloatFrom 0 (-1e6) (1e6))
  TIMESTAMP ->
    Timestamp <$>
      (Time.Timestamp <$> Gen.integral (Range.linearFrom 0 (-100000) (100000)) <*> Gen.integral (Range.linearFrom 0 0 999999999))
  DECIMAL ->
    (Decimal . fromInteger) <$> Gen.integral (Range.linearFrom 0 (-10000) (10000))
  DATE ->
    Date . Time.Day <$> Gen.integral (Range.linearFrom 0 (-100000) (100000))
  STRING ->
    String <$> Gen.element (Corpus.glass)
  BINARY ->
    Binary <$> Gen.element (Corpus.agile)
  VARCHAR ->
    VarChar <$> Gen.element (Corpus.nfl)
  CHAR ->
    Char <$> Gen.element (Corpus.nhl)
  LIST lt ->
    List . fromList <$> Gen.list (Range.linear 1 10) (genLogical lt)
  MAP kt vt ->
    Map . fromList <$> Gen.list (Range.linear 1 10) ((,) <$> genLogical kt <*> genLogical vt)
  UNION uts -> do
    n <- Gen.integral $ Range.constant 0 (List.length uts - 1)
    Union (fromIntegral n) <$> genLogical (uts List.!! n)
  STRUCT sts ->
    Struct . fromList <$> traverse (traverse genLogical) sts



-----------------
----- Tests -----
-----------------


prop_logical_tables_round_trip_via_stipes :: Property
prop_logical_tables_round_trip_via_stipes = withTests 1000 . property $ do
  typ       <- forAll genType
  logical   <- forAll $ fromList <$> Gen.list (Range.linear 0 100) (genLogical typ)

  striped   <- evalM      $ evalEither $ Convert.fromLogical typ logical
  recreated <- eval       $ Convert.toLogical striped

  logical === recreated


prop_logical_tables_roundtrip_via_files :: Property
prop_logical_tables_roundtrip_via_files = withTests 1000 . property $ do
  typ       <- forAll $ Gen.prune genType
  logical   <- forAll $ Gen.prune $ Gen.list (Range.linear 1 10) (genLogical typ)

  hoist runResourceT $ test $ do
    (_, dir)  <- Temp.createTempDirectory Nothing "prop_dir"
    let tfile  = dir </> "test.orc"

    evalExceptT . hoist (liftIO) $
      Binary.putOrcStream typ Nothing 100 tfile $
        Streaming.each logical

    recreated :> () <-
      evalExceptT . hoist (liftIO) $
        Binary.withOrcStream tfile $ \_ ->
          Streaming.toList

    logical === recreated


tests :: IO Bool
tests =
  checkSequential $$(discover)
