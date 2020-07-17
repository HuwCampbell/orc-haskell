{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}

module Orc.Serial.Binary.Logical (
    withOrcStream
  , printOrcFile
) where

import           Control.Monad.IO.Class
import           Control.Monad.Trans.Either (EitherT)

import           Data.String (String)

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Data.ByteString.Streaming as ByteStream

import           Orc.Serial.Binary.Striped
import           Orc.Serial.Json.Logical

import           Orc.Table.Convert (streamLogical)
import qualified Orc.Table.Logical as Logical

import           System.IO as IO

import           Orc.Prelude


-- | Stream the values as a logical rows.
--
--   This is the most useful way to read an ORC file,
--   but entails a pivot of all the values. If speed is
--   key, one may wish to use  withOrcStripes and do a
--   predicate pushdown first.
withOrcStream
  :: MonadIO m
  => FilePath
  -> ((Streaming.Stream (Of Logical.Row) (EitherT String m) ()) -> EitherT String IO r)
  -> EitherT String IO r
withOrcStream fs action =
  withOrcStripes fs $
    action . streamLogical


-- | Simple pretty printer of ORC to JSON.
--
-- Serves a a demonstration of how to grab an ORC file and
-- do something useful with it.
printOrcFile :: FilePath -> EitherT String IO ()
printOrcFile fp = do
  withOrcStream fp $
    ByteStream.stdout
      . ByteStream.concat
      . Streaming.maps (\(x :> r) -> ByteStream.fromLazy (ppJsonRow x) $> r)
