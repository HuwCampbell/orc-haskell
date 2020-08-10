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
  , hyloByteStream'
  , streamingPut

  , toVector
  , toVectorN
  , resizeChunks
) where

import           Orc.Prelude

import           Control.Monad.IO.Class
import           Control.Monad.Primitive (PrimMonad)

import           Data.Serialize.Put (PutM)
import qualified Data.Serialize.Put as Put
import qualified Data.List as List

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Streaming.Prelude as Streaming
import qualified Streaming.Internal as Streaming

import qualified Data.ByteString as Strict
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
          go (n + fromIntegral (Strict.length s)) rest
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


hyloByteStream' :: Monad m => (a -> ByteStream m ()) -> Streaming.Stream (Of a) m r -> ByteStream m r
hyloByteStream' step =
    loop
  where
    loop = \case
      Streaming.Return r ->
        ByteStream.Empty r

      Streaming.Effect m ->
        ByteStream.mwrap $
          loop <$> m

      Streaming.Step (a :> rest) -> do
        step a
        loop rest


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


resizeChunks
  :: MonadIO m
  => Int
  -> ByteStream m r -> Streaming.Stream (Of Strict.ByteString) m r
resizeChunks maxSize =
  loop (0, []) . ByteStream.toChunks
    where
  make =
    Strict.concat . List.reverse

  loop (n0,bss) incoming0 =
    if n0 >= maxSize then
      let
        (emit, retain) =
          Strict.splitAt maxSize (make bss)
      in
        Streaming.Step $
          emit :> loop (Strict.length retain, [retain]) incoming0

    else do
      ex <- Streaming.lift $ Streaming.next incoming0
      case ex of
        Left r -> do
          when (n0 /= 0 ) $
            Streaming.yield (make bss)

          pure r

        Right (hd, tl) ->
          loop (n0 + Strict.length hd, hd:bss) tl
