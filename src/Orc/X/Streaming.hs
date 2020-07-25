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

  , toVector
  , toVectorN
) where

import           Orc.Prelude

import           Control.Monad.IO.Class
import           Control.Monad.Primitive (PrimMonad)

import           Data.Serialize.Put (PutM)
import qualified Data.Serialize.Put as Put

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Streaming.Prelude as Streaming
import qualified Streaming.Internal as Streaming

import qualified Data.ByteString as ByteString
import qualified Data.ByteString.Streaming as ByteStream
import qualified Data.ByteString.Streaming.Internal as ByteStream

import qualified Data.Vector.Generic         as Vector
import qualified Data.Vector.Generic.Mutable as MVector

type ByteStream = ByteStream.ByteString



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


toVectorN :: (PrimMonad m, Vector.Vector v a) => Int -> Streaming.Stream (Of a) m r -> m (Of (v a) r)
toVectorN i s = do
  mv <- MVector.unsafeNew i
  let
    begin = do
      return 0
    step idx a = do
      MVector.write mv idx a
      return (idx + 1)
    done idx = do
      Vector.unsafeFreeze (MVector.take idx mv)
  Streaming.foldM step begin done s
{-# INLINABLE toVectorN #-}


toVector :: (PrimMonad m, Vector.Vector v a) => Int -> Streaming.Stream (Of a) m r -> m (Of (v a) r)
toVector i s =
  let
    begin = do
      mv <- MVector.unsafeNew i
      return (mv, 0)
    step (mv, idx) a = do
      let len = MVector.length mv
      mv' <- if idx >= len then MVector.unsafeGrow mv len else return mv
      MVector.unsafeWrite mv' idx a
      return (mv', (idx + 1))
    done (mv, idx) = do
      Vector.freeze (MVector.take idx mv)
  in
    Streaming.foldM step begin done s
{-# INLINABLE toVector #-}
