{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE FlexibleContexts    #-}
{- | Streaming of ORC files as 'Logical.Row'.

-}
module Orc.Serial.Binary.Logical (
    withOrcFile
  , putOrcFile
  , printOrcFile
) where

import           Control.Monad.Trans.Either (EitherT)

import           Data.String (String)

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Streaming.Prelude as Streaming
import qualified Data.ByteString.Streaming as ByteStream

import           Orc.Schema.Types
import           Orc.Serial.Binary.Base (MonadTransIO)
import qualified Orc.Serial.Binary.Striped as Striped
import           Orc.Serial.Json.Logical

import           Orc.Table.Convert (streamLogical, streamFromLogical)
import qualified Orc.Table.Logical as Logical

import           System.IO as IO

import           Orc.Prelude

-- | Open an ORC file and stream its values values as logical rows.
--
--   This is the most useful way to read an ORC file, and entails streaming
--   each row individually. If speed is key, one may wish to use the function
--   'Striped.withOrcFile' from "Orc.Serial.Binary.Striped" and do a predicate
--   pushdown first.
--
--   This function is lifted to a constrained 'MonadTransControl' type,
--   for IO and error reporting, which is further specialized to
--   @EitherT String IO@.
withOrcFile
  :: MonadTransIO t
  => FilePath
  -- ^ The ORC file to open
  -> (Type -> (Streaming.Stream (Of Logical.Row) (t IO) ()) -> t IO r)
  -- ^ How to consume the stream of values as a continuation
  -> t IO r
withOrcFile fs action =
  Striped.withOrcFile fs $ \typ ->
    action typ .
      streamLogical .
        Streaming.map snd

{-# SPECIALIZE
  withOrcFile
    :: FilePath
    -> (Type -> (Streaming.Stream (Of Logical.Row) (EitherT String IO) ())
    -> EitherT String IO r)
    -> EitherT String IO r #-}


-- | Simple pretty printer of ORC to JSON.
--
--   Serves a a demonstration of how to grab an ORC file and
--   do something useful with it, as well as a way to easily
--   pass an ORC file into @jq@ or a similar tool for ad-hoc
--   processing.
printOrcFile :: FilePath -> EitherT String IO ()
printOrcFile fp = do
  withOrcFile fp $ \_ ->
    ByteStream.stdout
      . ByteStream.concat
      . Streaming.maps (\(x :> r) -> ppJsonRow x $> r)



-- | Write a stream of values as an ORC file.
--
--   This function is lifted to a constrained 'MonadTransControl' type,
--   for IO and error reporting, which is further specialized to
--   @EitherT String IO@.
putOrcFile
  :: MonadTransIO t
  => Type
  -- ^ The types of the 'Logical.Row'
  -> Maybe CompressionKind
  -- ^ An optional compression standard to use
  -> Int
  -- ^ The number of rows in each stripe
  -> FilePath
  -- ^ The filepath to write to
  -> Streaming.Stream (Of Logical.Row) (t IO) ()
  -- ^ The stream of 'Logical.Row' to write
  -> t IO ()
putOrcFile typ mCompression chunkSize fp =
  Striped.putOrcFile (Just typ) mCompression fp .
    streamFromLogical chunkSize typ

{-# SPECIALIZE
  putOrcFile
    :: Type
    -> Maybe CompressionKind
    -> Int
    -> FilePath
    -> Streaming.Stream (Of Logical.Row) (EitherT String IO) ()
    -> EitherT String IO () #-}
