{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}

module Orc.Seek (
    bashFile
  , bashFileType
) where

import           Control.Monad.IO.Class
import           Control.Monad.Trans.Control (MonadTransControl (..))
import           Control.Monad.Except (MonadError, liftEither, throwError)
import           Control.Monad.State (MonadState (..), StateT (..), evalStateT, modify')
import           Control.Monad.Reader (ReaderT (..), runReaderT)

import qualified Data.Serialize.Get as Get

import           Data.Word (Word64)
import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           X.Control.Monad.Trans.Either (EitherT, newEitherT, left)

import           Viking (Of (..), Stream)
import qualified Viking.Stream as Viking

import           Orc.Data.Segmented
import           Orc.Data.Data (StructField (..), Indexed, currentIndex, currentValue, nextIndex, makeIndexed)
import           Orc.Data.Striped (Column (..))
import           Orc.Schema.Types as Orc
import           Orc.Encodings.Bytes
import           Orc.Encodings.Integers

import           System.IO as IO

import           P


withFileLifted
  :: (Monad (t IO), MonadTransControl t)
  => FilePath
  -> IOMode
  -> (Handle -> t IO r)
  -> t IO r
withFileLifted file mode action =
  liftWith (\run -> withFile file mode (run . action)) >>=
    restoreT . return


bashFileType :: FilePath -> EitherT String IO Type
bashFileType file =
  withFileLifted file ReadMode $ \handle -> do
    (postScript, footer) <-
      checkMagic handle

    let
      stripeInfos =
        stripes footer

      typeInfo =
        types footer

    return typeInfo


bashFile :: FilePath -> Viking.Stream (Of (StripeInformation, RowIndex, StripeFooter, Column)) (EitherT String IO) ()
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

      fmap Viking.each $
        for stripeInfos $ readStripe typeInfo (compression postScript) handle


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

  footer <- liftEither $ readFooter footerData

  liftIO  $ hSeek handle AbsoluteSeek 3

  return (postScript, footer)


readCompressedStream :: Maybe CompressionKind -> ByteString -> Either String ByteString
readCompressedStream = \case
  Nothing ->
    Right
  Just NONE ->
    Right
  _ -> const (Left "Unsupported Compression Kind")


readStripe :: MonadIO m => Type -> Maybe CompressionKind -> Handle -> StripeInformation -> EitherT String m (StripeInformation, RowIndex, StripeFooter, Column)
readStripe typeInfo mCompressionInfo handle stripeInfo = do

  --
  -- Set the Handle to the start of the
  -- stripe if it's specified. Otherwise,
  -- we should be able to assume it's
  -- contiguous with the last one.
  for_ (offset stripeInfo) $
    liftIO .
      hSeek handle AbsoluteSeek .
        fromIntegral

  riLength <-
    liftMaybe "Required Field Missing"
      (indexLength stripeInfo)

  rdLength <-
    liftMaybe "Required Field Missing"
      (dataLength stripeInfo)

  rsLength <-
    liftMaybe "Required Field Missing"
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

    columnsEncodings =
      columns stripeFooter

  column <-
    decodeColumnTop typeInfo mCompressionInfo columnsEncodings nonRowIndexStreams dataBytes

  return (stripeInfo, rowIndex, stripeFooter, column)


decodeColumnTop :: MonadIO m => Type -> Maybe CompressionKind -> [Orc.ColumnEncoding] -> [Orc.Stream] -> ByteString -> EitherT String m Column
decodeColumnTop types mCompression encodings streams dataBytes =
  evalStateT (runReaderT (decodeColumn types) mCompression) (makeIndexed encodings, streams, dataBytes)


type OrcDecode m = ReaderT (Maybe CompressionKind) (StateT (Indexed Orc.ColumnEncoding, [Orc.Stream], ByteString) (EitherT String m))


withPresence :: Monad m => OrcDecode m (Column -> Column)
withPresence = do
  (ix, streams, bytes) <- get
  case streams of
    s:remainingStreams | streamColumn s == Just (currentIndex ix) && streamKind s == Just SK_PRESENT -> do

      (_, presenceBytes) <-
        popStream

      presenceColumn <-
        liftEither $
          decodeBytes presenceBytes

      return $ Partial presenceColumn
    _ -> return id


popStream :: Monad m => OrcDecode m (Orc.Stream, ByteString)
popStream = do
  (ix, streams, bytes) <- get
  case streams of
    s:rest -> do
      colLength <-
        liftMaybe "Need Column Length" $
          streamLength s
      let
        (theseBytes, remainingBytes) =
          ByteString.splitAt (fromIntegral colLength) bytes

      put (ix, rest, remainingBytes)
      return $! (s, theseBytes)

    _ -> throwError "No data to pop"


incrementColumn :: Monad m => OrcDecode m ()
incrementColumn =
  modify' $ \(ix, a, b) -> (nextIndex ix, a, b)


decodeColumn :: Monad m => Type -> OrcDecode m Column
decodeColumn types =
      withPresence <*> decodeColumnPart types <* incrementColumn


currentEncoding :: Monad m => OrcDecode m (Orc.ColumnEncoding)
currentEncoding = do
  (ix, _, _) <- get
  liftMaybe "Encodings Exhausted"
    (currentValue ix)


-- | Read A Column, Present Column has already been handled.
decodeColumnPart :: Monad m => Type -> OrcDecode m Column
decodeColumnPart types = do
  mEncodingKind <-
    columnEncodingKind <$> currentEncoding

  encodingKind <-
    liftMaybe "Encoding Kinds required"
      mEncodingKind

  case (types, encodingKind) of
    (BOOLEAN, _) -> do
      (_, presenceBytes) <-
        popStream

      le'column <-
        liftEither $
            decodeBytes presenceBytes

      return $
        Bool le'column


    (BYTE, _) -> do
      (_, dataBytes) <- popStream
      bytes          <- liftEither (decodeBytes dataBytes)
      return $ Bytes bytes

    (SHORT, DIRECT) -> do
      (_, dataBytes) <- popStream
      Integer <$> liftEither (decodeIntegerRLEv1 dataBytes)

    (INT, DIRECT) -> do
      (_, dataBytes) <- popStream
      Integer <$> liftEither (decodeIntegerRLEv1 dataBytes)

    (LONG, DIRECT) -> do
      (_, dataBytes) <- popStream
      Integer <$> liftEither (decodeIntegerRLEv1 dataBytes)

    (SHORT, DIRECT_V2) -> do
      (_, dataBytes) <- popStream
      Integer . Storable.map fromIntegral <$> liftEither (decodeIntegerRLEv2 dataBytes)

    (INT, DIRECT_V2) -> do
      (_, dataBytes) <- popStream
      Integer . Storable.map fromIntegral <$> liftEither (decodeIntegerRLEv2 dataBytes)

    (LONG, DIRECT_V2) -> do
      (_, dataBytes) <- popStream
      Integer . Storable.map fromIntegral <$> liftEither (decodeIntegerRLEv2 dataBytes)

    (FLOAT, _) -> do
      (_, dataBytes) <- popStream
      floats <- liftEither (decodeFloat32 dataBytes)
      return $ Float floats

    (DOUBLE, _) -> do
      (_, dataBytes) <- popStream
      doubles <- liftEither (decodeFloat64 dataBytes)
      return $ Double doubles

    (STRING, encoding) ->
      String <$> decodeString encoding

    (CHAR, encoding) ->
      Char <$> decodeString encoding

    (VARCHAR, encoding) ->
      VarChar <$> decodeString encoding

    (STRUCT fields, _) ->
      incrementColumn >>
        decodeStruct fields

    (DECIMAL, enc) -> do
      (_, dataBytes)  <- popStream
      (_, scaleBytes) <- popStream
      words           <- liftEither (decodeBase128Varint dataBytes)
      scale           <-
        case enc of
          DIRECT ->
            liftEither (decodeIntegerRLEv1 scaleBytes)
          _ ->
            liftEither (decodeIntegerRLEv2 scaleBytes)

      return $ Decimal words scale

    (u,k) -> pure $ UnhandleColumn u k




decodeString :: Monad m => Orc.ColumnEncodingKind -> OrcDecode m (Boxed.Vector ByteString)
decodeString = \case
  DIRECT -> do
    (_, dataBytes)   <- popStream
    (_, lengthBytes) <- popStream
    lengths <-
      liftEither (decodeIntegerRLEv1 lengthBytes)

    return $!
      bytesOfSegmented $!
        splitByteString lengths dataBytes

  DICTIONARY -> do
    (_, dataBytes)       <- popStream
    (_, lengthBytes)     <- popStream
    (_, dictionaryBytes) <- popStream

    selections :: Storable.Vector Word64 <-
      liftEither $
        decodeIntegerRLEv1 dataBytes

    lengths :: Storable.Vector Word64 <-
      liftEither $
        decodeIntegerRLEv1 lengthBytes

    let
      dictionary =
        Boxed.convert . bytesOfSegmented $
          splitByteString lengths dictionaryBytes

      discovered =
        Boxed.map (\i -> fromMaybe "SHIT" (dictionary Boxed.!? (fromIntegral i))) $
          Boxed.convert selections

    return $!
      discovered

  DIRECT_V2 -> do
    (_, dataBytes)   <- popStream
    (_, lengthBytes) <- popStream
    lengths <-
      liftEither (decodeIntegerRLEv2 lengthBytes)

    return $!
      bytesOfSegmented $!
        splitByteString lengths dataBytes

  DICTIONARY_V2 -> do
    (_, dataBytes)       <- popStream
    (_, dictionaryBytes) <- popStream
    (_, lengthBytes)     <- popStream

    selections <-
      liftEither (decodeIntegerRLEv2 dataBytes)

    lengths :: Storable.Vector Word64 <-
      liftEither (decodeIntegerRLEv2 lengthBytes)

    let
      dictionary =
        Boxed.convert . bytesOfSegmented $
          splitByteString lengths dictionaryBytes

      discovered =
        Boxed.map (\i -> fromMaybe "" (dictionary Boxed.!? (fromIntegral i))) $
          Boxed.convert selections

    return $
      discovered


decodeStruct :: Monad m => [StructField Type] -> OrcDecode m Column
decodeStruct fields =
  fmap Struct $
    for fields $
      traverse decodeColumn


liftMaybe :: MonadError e m => e -> Maybe a -> m a
liftMaybe =
  liftEither ... note


(...) :: (c -> d) -> (a -> b -> c) -> a -> b -> d
(...) = (.) . (.)


note :: a -> Maybe b -> Either a b
note x = maybe (Left x) Right
