{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Viking.Stream.Cereal (
    runGet
  , runGetSome
  , runGetMany

  , BinaryError(..)
  , renderBinaryError
  ) where

import           Control.Monad.Morph (hoist)
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Either (EitherT, left)

import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get
import qualified Data.ByteString as Strict
import qualified Data.Text as Text

import           P

import           Viking
import qualified Viking.ByteStream as ByteStream
import qualified Viking.Stream as Stream

data BinaryError
  = BinaryDecodeError Text
  | BinaryExplosion
  deriving (Eq, Show)

renderBinaryError :: BinaryError -> Text
renderBinaryError = \case
  BinaryDecodeError msg ->
    "Binary decode error: " <> msg

-- | Run a 'Get' binary decoder over a 'ByteStream'.
--
--   Return decoded value, as well as the rest of the stream.
runGet :: Monad m => Get a -> ByteStream m r -> EitherT BinaryError m (a, ByteStream m r)
runGet get input =
  let
    loop bss0 = \case
      Get.Fail err _bs ->
        left . BinaryDecodeError $ Text.pack err

      Get.Done x bs ->
        pure (x, ByteStream.consChunk bs bss0)

      Get.Partial k -> do
        e <- lift $ ByteStream.nextChunk bss0
        case e of
          Left r ->
            loop (pure r) (k "")

          Right (bs, bss) ->
            loop bss (k bs)

  in do
    e <- lift $ ByteStream.nextChunk input
    case e of
      Left r ->
        loop (pure r) (Get.runGetPartial get "")
      Right (bs, bss) ->
        loop bss (Get.runGetPartial get bs)

-- | Run a 'Get' binary decoder over a 'ByteStream' one or more times.
--
runGetSome :: Monad m => Get a -> ByteStream m r -> Stream (Of a) (EitherT BinaryError m) r
runGetSome get input =
  let
    nextGet bss0 = do
      e <- lift $ ByteStream.nextChunk bss0
      case e of
        Left r ->
          pure r
        Right (bs, bss) ->
          loop bss (Get.runGetPartial get bs) -- `Get.pushChunk` bs)

    loop bss0 = \case
      Get.Fail err _bs ->
        lift . left . BinaryDecodeError $ Text.pack err

      Get.Done x bs -> do
        Stream.yield x
        if Strict.null bs then
          nextGet bss0
        else
          loop bss0 (Get.runGetPartial get bs) -- `Get.pushChunk` bs)

      Get.Partial k -> do
        e <- lift $ ByteStream.nextChunk bss0
        case e of
          Left r ->
            loop (pure r) (k mempty)

          Right (bs, bss) ->
            loop bss (k bs)
  in
    loop (hoist lift input) (Get.runGetPartial get mempty)

-- | Run a 'Get' binary decoder over a 'ByteStream' zero or more times.
--
runGetMany :: Monad m => Get a -> ByteStream m r -> Stream (Of a) (EitherT BinaryError m) r
runGetMany get input = do
  e <- lift . lift $ ByteStream.nextChunk input
  case e of
    Left r ->
      pure r
    Right (hd, tl) ->
      if Strict.null hd then
        runGetMany get tl
      else
        runGetSome get (ByteStream.consChunk hd tl)
