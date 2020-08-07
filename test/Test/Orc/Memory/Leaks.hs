{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
module Test.Orc.Memory.Leaks (
    tests
) where

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

import           Control.Monad.Morph
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Resource (runResourceT)

import           Data.Maybe
import           Data.Foldable
import           Orc.Prelude

import qualified Orc.Serial.Binary.Base as Base
import qualified Orc.Serial.Binary.Striped as Striped
import qualified Orc.Serial.Binary.Logical as Logical

import           Test.Orc.Type (genType, genCompressionKind)
import           Test.Orc.Logical (genLogical, genLogical')

import           Streaming (Of (..))
import qualified Streaming.Prelude as Streaming

import qualified System.IO.Temp as Temp
import           System.FilePath ((</>))

import           System.Mem
import           System.Mem.Weak

import qualified Prelude as Savage

prop_logical_tables_don't_leak :: Property
prop_logical_tables_don't_leak = property $ do
  typ        <- forAll genType
  cmpKind    <- forAll genCompressionKind
  stripeSize <- forAll $ Gen.int  (Range.linear 1 10)
  logical    <- forAll $ Gen.list (Range.linear 0 100) (genLogical' typ)

  hoist runResourceT $ test $ do
    (_, dir)  <- Temp.createTempDirectory Nothing "prop_dir"
    let tempFile   = dir </> "test.orc"
    let tempFile2  = dir </> "test2.orc"

    evalIO $
      Logical.putOrcFile typ cmpKind stripeSize tempFile $
        Streaming.each logical

    weaks :> () <- evalIO . Base.raisingOrcErrors $
      Logical.withOrcFileLifted tempFile $ \_  ->
        Logical.putOrcFileLifted typ cmpKind stripeSize tempFile2 .
          Streaming.store (Streaming.toList . Streaming.mapM (liftIO . (mkWeakPtr `flip` Nothing)))

    evalIO performGC
    rows <- evalIO $ traverse deRefWeak weaks

    annotateShow rows
    assert $
      Orc.Prelude.all isNothing rows

prop_striped_tables_don't_leak :: Property
prop_striped_tables_don't_leak = property $ do
  typ        <- forAll genType
  cmpKind    <- forAll genCompressionKind
  stripeSize <- forAll $ Gen.int  (Range.linear 1 10)
  logical    <- forAll $ Gen.list (Range.linear 0 100) (genLogical typ)

  hoist runResourceT $ test $ do
    (_, dir)  <- Temp.createTempDirectory Nothing "prop_dir"
    let tempFile   = dir </> "test.orc"
    let tempFile2  = dir </> "test2.orc"

    evalIO $
      Logical.putOrcFile typ cmpKind stripeSize tempFile $
        Streaming.each logical

    weaks :> () <- evalIO . Base.raisingOrcErrors $
      Striped.withOrcFileLifted tempFile $ \_  ->
        Striped.putOrcFileLifted (Just typ) cmpKind tempFile2 .
          Streaming.map snd .
            Streaming.store (Streaming.toList . Streaming.mapM (liftIO . (mkWeakPtr `flip` Nothing)))

    evalIO performGC
    rows <- evalIO $ traverse deRefWeak weaks

    annotateShow rows
    assert $
      Orc.Prelude.all isNothing rows


prop_striped_tables_don't_leak_during :: Property
prop_striped_tables_don't_leak_during = property $ do
  typ        <- forAll genType
  cmpKind    <- forAll genCompressionKind
  stripeSize <- forAll $ Gen.int  (Range.linear 1 10)
  logical    <- forAll $ Gen.list (Range.linear 0 100) (genLogical typ)

  hoist runResourceT $ test $ do
    (_, dir)  <- Temp.createTempDirectory Nothing "prop_dir"
    let tempFile   = dir </> "test.orc"
    let tempFile2  = dir </> "test2.orc"

    evalIO $
      Logical.putOrcFile typ cmpKind stripeSize tempFile $
        Streaming.each logical

    () :> () <- evalIO . Base.raisingOrcErrors $
      Striped.withOrcFileLifted tempFile $ \_ stream ->
        Striped.putOrcFileLifted (Just typ) cmpKind tempFile2 $
          Streaming.map snd $
            flip Streaming.store stream $
              Streaming.foldM (\x a -> do
                liftIO performGC
                d <- liftIO (traverse deRefWeak x)
                when (any isJust d) (Savage.error "Held onto")
                Just <$> liftIO (mkWeakPtr a Nothing)
              ) (pure Nothing) (\x -> do
                liftIO performGC
                d <- liftIO (traverse deRefWeak x)
                when (any isJust d) (Savage.error "Held onto")
              )

    evalIO performGC

tests :: IO Bool
tests =
  checkSequential $$(discover)
