{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}

module Orc.X.Streaming (
    streamingLength
  , hyloByteStream
  , streamingPut
  , rechunk
) where

import           Orc.Prelude

import           Control.Monad.IO.Class

import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Lazy as Lazy

import           Data.Serialize.Put (PutM)
import qualified Data.Serialize.Put as Put

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Streaming.Internal as Streaming

import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Streaming as ByteStream
import qualified Data.ByteString.Streaming.Internal as ByteStream

type ByteStream = ByteStream.ByteString


rechunk :: Monad m => Int -> ByteStream m a -> ByteStream m a
rechunk sz =
  let
    mkAcc = Builder.byteString
    mkStrict = Lazy.toStrict . Builder.toLazyByteString

    go acc i bs =
      case bs of
        ByteStream.Empty r ->
          ByteStream.Empty r

        ByteStream.Chunk s rest ->
          let
            ss = ByteString.length s
          in
            if (ss + i >= sz) then
              let
                (emit, next) =
                  ByteString.splitAt (sz - i) s

              in
                ByteStream.Chunk (mkStrict (acc <> mkAcc emit)) $
                  go mempty 0 $ ByteStream.Chunk next rest

            else
              go (acc <> mkAcc s) (i + ss) rest

        ByteStream.Go act ->
          ByteStream.Go $
            fmap (go acc i) act

  in
    go mempty 0


streamingLength :: Monad m => ByteStream m a -> ByteStream m (Of Word64 a)
streamingLength =
  go 0
    where
  go !n bs =
    case bs of
      ByteStream.Empty r ->
        ByteStream.Empty (n :> r)
      ByteStream.Chunk s rest ->
        ByteStream.Chunk s $
          go (n + fromIntegral (ByteString.length s)) rest
      ByteStream.Go act ->
        ByteStream.Go $
          fmap (go n) act
{-# INLINE streamingLength #-}


hyloByteStream :: Monad m => (x -> a -> ByteStream m x) -> x -> Streaming.Stream (Of a) m r -> ByteStream m (Of x r)
hyloByteStream step begin =
    loop begin
  where
    loop x = \case
      Streaming.Return r ->
        ByteStream.Empty (x :> r)

      Streaming.Effect m ->
        ByteStream.mwrap $
          loop x <$> m

      Streaming.Step (a :> rest) -> do
        x' <- step x a
        loop x' rest



streamingPut :: MonadIO m => PutM a -> ByteStream m a
streamingPut x =
  let (a, bldr) = Put.runPutMBuilder x
  in  ByteStream.toStreamingByteString bldr $> a
{-# INLINE streamingPut #-}
