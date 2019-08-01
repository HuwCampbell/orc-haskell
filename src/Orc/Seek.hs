{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}

module Orc.Seek (
  bashFile
) where

import           Control.Monad.IO.Class
import           Control.Monad.Trans.Control (MonadTransControl (..))
import           Control.Monad.Trans.State (StateT (..), runStateT)

import qualified Data.Serialize.Get as Get

import           Data.Word (Word32)
import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString

import           X.Control.Monad.Trans.Either (EitherT, newEitherT, left, hoistMaybe, hoistEither)

import           Viking (Of (..), Stream)
import qualified Viking.Stream as Viking

import           Orc.Data.Data (StructField (..))
import           Orc.Data.Striped (Column (..))
import           Orc.Schema.Types as Orc
import           Orc.Encodings.Bytes
import           Orc.Encodings.Integers

import           P

import           System.IO as IO
import           Text.Show.Pretty (ppShow)


withFileLifted
  :: (Monad (t IO), MonadTransControl t)
  => FilePath
  -> IOMode
  -> (Handle -> t IO r)
  -> t IO r
withFileLifted file mode action =
  liftWith (\run -> withFile file mode (run . action)) >>=
    restoreT . return


bashFile :: FilePath -> Viking.Stream (Of (StripeInformation, RowIndex, StripeFooter)) (EitherT String IO) ()
bashFile file =
  Viking.effect $
    withFileLifted file ReadMode $ \handle -> do
      (postScript, footer) <-
        checkMagic handle

      let
        stripeInfos =
          stripes footer

        typeInfo =
          types footer

      liftIO . putStrLn . ppShow $ postScript
      liftIO . putStrLn . ppShow $ footer

      fmap Viking.each $
        for stripeInfos $ readStripeMeta typeInfo handle


-- | Checks that the magic values are present in the file
--   (i.e. makes sure that it is actually an ORC file)
checkMagic :: MonadIO m => Handle -> EitherT String m (PostScript, Footer)
checkMagic handle = do
  liftIO  $ hSeek handle AbsoluteSeek 0
  header <- liftIO (ByteString.hGet handle 3)

  unless (header == "ORC") $
    left (show header)

  -- Seek to the last byte of the file to get the
  -- size of the postscript.
  liftIO  $
    hSeek handle SeekFromEnd (-1)

  psLength <-
    newEitherT $ Get.runGet Get.getWord8 <$> liftIO (ByteString.hGet handle 1)

  liftIO $
    hSeek handle SeekFromEnd (negate $ 1 + fromIntegral psLength)

  postScript <-
    newEitherT $ readPostScript <$> liftIO (ByteString.hGet handle $ fromIntegral psLength)

  let
    compressionInfo =
      compression postScript

    footerLen =
      footerLength postScript

  liftIO $ hSeek handle SeekFromEnd (negate $ 1 + fromIntegral psLength + fromIntegral footerLen)
  footerData <- newEitherT $ readCompressedStream compressionInfo <$> liftIO (ByteString.hGet handle $ fromIntegral footerLen)

  footer <- hoistEither $ readFooter footerData

  liftIO  $ hSeek handle AbsoluteSeek 3

  return (postScript, footer)


readCompressedStream :: Maybe CompressionKind -> ByteString -> Either String ByteString
readCompressedStream = \case
  Nothing ->
    Right
  Just NONE ->
    Right
  _ -> const (Left "Unsupported Compression Kind")

readStripeMeta :: MonadIO m => Type -> Handle -> StripeInformation -> EitherT String m (StripeInformation, RowIndex, StripeFooter)
readStripeMeta typeInfo handle stripeInfo = do

  riLength <-
    hoistMaybe "Required Field Missing"
      (indexLength stripeInfo)

  rdLength <-
    hoistMaybe "Required Field Missing"
      (dataLength stripeInfo)

  rsLength <-
    hoistMaybe "Required Field Missing"
      (siFooterLength stripeInfo)

  rowIndex <-
    newEitherT $ readRowIndex <$> liftIO (ByteString.hGet handle $ fromIntegral riLength)

  dataBytes <-
    liftIO (ByteString.hGet handle $ fromIntegral rdLength)

  stripeFooter <-
    newEitherT $ readStripeFooter <$> liftIO (ByteString.hGet handle $ fromIntegral rsLength)

  let
    nonRowIndexStreams =
      drop (length (entry rowIndex)) $
        streams stripeFooter

  -- liftIO . print . fmap Storable.length $
  --   decodeBits 6000 (ByteString.take 12 dataBytes)

  -- liftIO . print . fmap Storable.length $
  --   decodeWord64 4000 (ByteString.take 13673 $ ByteString.drop 12 dataBytes)

  -- column <-
  --   decodeColumn typeInfo nonRowIndexStreams

  return (stripeInfo, rowIndex, stripeFooter)



decodeColumnTop :: MonadIO m => Type -> [Orc.Stream] -> ByteString -> EitherT String m Column
decodeColumnTop types streams dataBytes =
  fmap fst $ decodeNested types (0, streams, dataBytes)


decodeNested :: MonadIO m => Type -> (Word32, [Orc.Stream], ByteString) -> EitherT String m (Column, (Word32, [Orc.Stream], ByteString))
decodeNested types (ix, streams, dataBytes) =
  case types of
    STRUCT fields ->
      case streams of
        s:streams1 | streamColumn s == Just ix && streamKind s == Just SK_PRESENT -> do
          presenceColumnLength <-
            hoistMaybe "Need Column Length" $
              streamLength s
          let
            (presenceBytes, remaining) =
              ByteString.splitAt (fromIntegral presenceColumnLength) dataBytes

          presenceColumn <-
            hoistEither $
              decodeBytes 0 presenceBytes

          (structColumns, continuation) <-
            rumbleStruct (ix + 1) fields streams1 remaining

          pure
            (Partial presenceColumn structColumns, continuation)

        _ ->
          rumbleStruct (ix + 1) fields streams dataBytes




-- decodeDecimalColumn :: StripeInformation -> RowIndex -> StripeFooter -> ByteString -> Storable.Vector Word8
-- decodeDecimalColumn stripeInformation rowIndex stripeFooter =
--   decodeBytes (fromMaybe 0 $ siNumberOfRows stripeInformation)
rumbleStruct :: MonadIO m => Word32 -> [StructField Type] -> [Orc.Stream] -> ByteString -> EitherT String m (Column, (Word32, [Orc.Stream], ByteString))
rumbleStruct ix fields streams dataBytes =
  flip runStateT (ix, streams, dataBytes) $
    fmap Struct $
      for fields $
        traverse $
          StateT . decodeNested
