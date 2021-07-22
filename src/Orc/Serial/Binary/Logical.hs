{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE FlexibleContexts    #-}
{- | Streaming of ORC files as 'Logical.Row'.

     This module provides two separate mechanisms for dealing with
     invalid Orc files.

     To stream entirely in IO, one can just use @withOrcFile@ and
     @putOrcFile@
-}
module Orc.Serial.Binary.Logical (
  -- * IO interface
    withOrcFile
  , putOrcFile

  -- * lifted interface
  , printOrcFile
  , withOrcFileLifted
  , putOrcFileLifted
) where

import           Control.Monad.Trans.Either (EitherT)

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Streaming.Prelude as Streaming
import qualified Streaming.ByteString as ByteStream

import           Orc.Schema.Types
import           Orc.Serial.Binary.Base (MonadTransIO, OrcException, Raising (..))
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
--   'Striped.withOrcFileLifted' from "Orc.Serial.Binary.Striped" and do a predicate
--   pushdown first.
--
--   This function is lifted to a constrained 'MonadTransControl' type,
--   for IO and error reporting, which is further specialized to
--   @EitherT OrcException IO@, and @Raising IO@.
withOrcFileLifted
  :: MonadTransIO t
  => FilePath
  -- ^ The ORC file to open
  -> (Type -> (Streaming.Stream (Of Logical.Row) (t IO) ()) -> t IO r)
  -- ^ How to consume the stream of values as a continuation
  -> t IO r
withOrcFileLifted fs action =
  Striped.withOrcFileLifted fs $ \typ ->
    action typ .
      streamLogical .
        Streaming.map snd


{-# SPECIALIZE
  withOrcFileLifted
    :: FilePath
    -> (Type -> (Streaming.Stream (Of Logical.Row) (EitherT OrcException IO) ())
    -> EitherT OrcException IO r)
    -> EitherT OrcException IO r #-}

{-# SPECIALIZE
  withOrcFileLifted
    :: FilePath
    -> (Type -> (Streaming.Stream (Of Logical.Row) (Raising IO) ())
    -> Raising IO r)
    -> Raising IO r #-}


-- | Open an ORC file and stream its values values as logical rows.
--
--   Throws an OrcException if there is an issue with the file.
withOrcFile
  :: FilePath
  -- ^ The ORC file to open
  -> (Type -> (Streaming.Stream (Of Logical.Row) IO ()) -> IO r)
  -- ^ How to consume the stream of values as a continuation
  -> IO r
withOrcFile fs action =
  raisingOrcErrors $
    withOrcFileLifted fs $ \t s ->
      Raising . action t $
        Streaming.hoist raisingOrcErrors s


-- | Simple pretty printer of ORC to JSON.
--
--   Serves a a demonstration of how to grab an ORC file and
--   do something useful with it, as well as a way to easily
--   pass an ORC file into @jq@ or a similar tool for ad-hoc
--   processing.
printOrcFile :: FilePath -> IO ()
printOrcFile fp = do
  withOrcFile fp $ \_ ->
    ByteStream.stdout
      . ByteStream.concat
      . Streaming.maps (\(x :> r) -> ppJsonRow x $> r)



-- | Write a stream of values as an ORC file.
--
--   This function is lifted to a constrained 'MonadTransControl' type,
--   for IO and error reporting, which is further specialized to
--   @EitherT OrcException IO@, and @Raising IO@.
putOrcFileLifted
  :: MonadTransIO t
  => Type
  -- ^ The types of the 'Logical.Row'
  -> Maybe CompressionKind
  -- ^ An optional compression standard to use
  -> Int
  -- ^ The number of rows in each stripe
  -> FilePath
  -- ^ The filepath to write to
  -> Streaming.Stream (Of Logical.Row) (t IO) r
  -- ^ The stream of 'Logical.Row' to write
  -> t IO r
putOrcFileLifted typ mCompression chunkSize fp =
  Striped.putOrcFileLifted (Just typ) mCompression fp .
    streamFromLogical chunkSize typ

{-# SPECIALIZE
  putOrcFileLifted
    :: Type
    -> Maybe CompressionKind
    -> Int
    -> FilePath
    -> Streaming.Stream (Of Logical.Row) (EitherT OrcException IO) r
    -> EitherT OrcException IO r #-}


{-# SPECIALIZE
  putOrcFileLifted
    :: Type
    -> Maybe CompressionKind
    -> Int
    -> FilePath
    -> Streaming.Stream (Of Logical.Row) (Raising IO) r
    -> Raising IO r #-}


-- | Write a stream of values as an ORC file.
putOrcFile
  :: Type
  -- ^ The types of the 'Logical.Row'
  -> Maybe CompressionKind
  -- ^ An optional compression standard to use
  -> Int
  -- ^ The number of rows in each stripe
  -> FilePath
  -- ^ The filepath to write to
  -> Streaming.Stream (Of Logical.Row) IO r
  -- ^ The stream of 'Logical.Row' to write
  -> IO r
putOrcFile typ mCompression chunkSize fp s =
  raisingOrcErrors $
    putOrcFileLifted typ mCompression chunkSize fp $
      Streaming.hoist Raising s
