{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}

module Orc.Serial.Seek (
    openOrcFile
  , checkOrcFile

  , printOrcFile
) where

import           Control.Monad.IO.Class
import           Control.Monad.Except (MonadError, liftEither, throwError)
import           Control.Monad.State (MonadState (..), StateT (..), evalStateT, modify')
import           Control.Monad.Reader (ReaderT (..), runReaderT, ask)
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Control (MonadTransControl (..))
import           Control.Monad.Trans.Either (EitherT, newEitherT, left, runEitherT)
import           Control.Monad.Trans.Resource (MonadResource (..), runResourceT, allocate)

import qualified Data.Serialize.Get as Get

import           Data.List (dropWhile)
import           Data.Word (Word64)

import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.Text as Text
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable


import           Viking (Of (..), ByteStream)
import qualified Viking.Stream as Viking
import qualified Viking.ByteStream as ByteStream

import           Orc.Data.Segmented
import           Orc.Data.Data (StructField (..), Indexed, currentIndex, currentValue, nextIndex, makeIndexed, prevIndex)
import           Orc.Schema.Types as Orc
import           Orc.Serial.Encodings.Bytes
import           Orc.Serial.Encodings.Compression
import           Orc.Serial.Encodings.Integers
import           Orc.Serial.Encodings.OrcNum
import           Orc.Serial.Logical (streamLogical)
import           Orc.Table.Striped (Column (..))
import           Orc.Table.Logical (ppRow)

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


checkOrcFile :: FilePath -> EitherT String IO ([StripeInformation], Type, Maybe CompressionKind)
checkOrcFile file =
  withFileLifted file ReadMode $ \handle -> do
    (postScript, footer) <-
      checkMagic handle

    let
      stripeInfos =
        stripes footer

      typeInfo =
        types footer

    return (stripeInfos, typeInfo, (compression postScript))


printOrcFile :: FilePath -> IO ()
printOrcFile fp = do
  res <-
    runResourceT $
      runEitherT $ do
        Viking.stdoutLn $
          Viking.take 10 $
          Viking.map (Text.unpack . ppRow) $
            streamLogical $
              openOrcFile fp

  case res of
    Right () -> putStrLn "Done"
    Left err -> putStrLn err


openOrcFile :: MonadResource m => FilePath -> Viking.Stream (Of (StripeInformation, Column)) (EitherT String m) ()
openOrcFile file =
  Viking.effect $ do
    (_, handle) <-
      allocate (openFile file ReadMode) hClose

    (postScript, footer) <-
      checkMagic handle

    let
      stripeInfos =
        stripes footer

      typeInfo =
        types footer

    return $
      Viking.mapM
        (readStripe typeInfo (compression postScript) handle)
        (Viking.each stripeInfos)



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

  liftIO $
    hSeek handle SeekFromEnd (negate $ 1 + fromIntegral psLength + fromIntegral footerLen)

  footerData <-
    newEitherT $ readCompressedStream compressionInfo <$> liftIO (ByteString.hGet handle $ fromIntegral footerLen)

  footer <-
    liftEither $ readFooter footerData

  liftIO $
    hSeek handle AbsoluteSeek 3

  return (postScript, footer)


readStripe :: MonadIO m => Type -> Maybe CompressionKind -> Handle -> StripeInformation -> EitherT String m (StripeInformation, Column)
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

  --
  -- Jump over the row index streams.
  -- These are only really useful when doing filtering statistics, or skipping.
  liftIO $
    hSeek handle RelativeSeek (fromIntegral riLength)

  --
  -- One could read dataBytes into a Strict ByteString here (as that's the order in the file),
  -- but, that would take a bunch of RAM, so we're jumping over the data and reading the footer,
  -- then jumping back to the start (were we are now).
  dataBytesStart <-
    liftIO $
      hTell handle

  --
  -- Jumping forwards to footer
  liftIO $
    hSeek handle RelativeSeek (fromIntegral rdLength)

  stripeFooterData <-
    newEitherT $ readCompressedStream mCompressionInfo <$> liftIO (ByteString.hGet handle $ fromIntegral rsLength)

  stripeFooter <-
    liftEither $
      readStripeFooter stripeFooterData

  let
    nonRowIndexStreams =
      dropWhile (\s -> streamKind s == Just SK_ROW_INDEX || streamKind s == Just SK_BLOOM_FILTER) $
        streams stripeFooter

    columnsEncodings =
      columns stripeFooter

  -- Jump back to dataBytes
  liftIO $
    hSeek handle AbsoluteSeek dataBytesStart

  column <-
    decodeColumnTop typeInfo mCompressionInfo columnsEncodings nonRowIndexStreams $
      ByteStream.hGet handle $ fromIntegral rdLength

  return (stripeInfo, column)


decodeColumnTop :: MonadIO m => Type -> Maybe CompressionKind -> [Orc.ColumnEncoding] -> [Orc.Stream] -> ByteStream m () -> EitherT String m Column
decodeColumnTop typs mCompression encodings orcStreams dataBytes =
  evalStateT (runReaderT (decodeColumn typs) mCompression) (makeIndexed encodings, orcStreams, dataBytes)


type OrcDecode m = ReaderT (Maybe CompressionKind) (StateT (Indexed Orc.ColumnEncoding, [Orc.Stream], ByteStream m ()) (EitherT String m))


withPresence :: Monad m => OrcDecode m (Column -> Column)
withPresence = do
  (ix, streams0, _bytes) <- get
  case streams0 of
    s:_ | streamColumn s == Just (currentIndex ix) && streamKind s == Just SK_PRESENT -> do

      presenceBytes <-
        popStream

      presenceColumn <-
        liftEither $
          decodeBits presenceBytes

      return $ Partial presenceColumn

    _else ->
      return id


popStream :: Monad m => OrcDecode m ByteString
popStream = do
  compressionInfo <-
    ask

  (ix, orcStreams, bytes) <-
    get

  case orcStreams of
    s:rest | streamColumn s == Just (currentIndex ix) -> do
      colLength <-
        liftMaybe "Need Column Length" $
          streamLength s

      (uncompressedBytes :> remainingBytes) <-
        lift . lift . lift . ByteStream.toStrict $
          ByteStream.splitAt (fromIntegral colLength) bytes

      theseBytes <-
        liftEither $ readCompressedStream compressionInfo uncompressedBytes

      put (ix, rest, remainingBytes)
      return $! theseBytes

    _ -> throwError $ "No streams to pop in column: " <> show (currentIndex ix)


incrementColumn :: Monad m => OrcDecode m ()
incrementColumn =
  modify' $ \(ix, a, b) -> (nextIndex ix, a, b)


decrementColumn :: Monad m => OrcDecode m ()
decrementColumn =
  modify' $ \(ix, a, b) -> (prevIndex ix, a, b)


-- | Push inwards when running a nested column
--   such as a Struct, Map or List.
--   We need to pop out again, as the wrapping decodeColumn
--   will increment the column when complete.
nestedColumn :: Monad m => OrcDecode m a -> OrcDecode m a
nestedColumn f =
  incrementColumn *> f <* decrementColumn


currentEncoding :: Monad m => OrcDecode m (Orc.ColumnEncoding)
currentEncoding = do
  (ix, _, _) <- get
  maybe
    (throwError $ "Couldn't find Column encoding for column: " <> show ix)
    pure
    (currentValue ix)


-- | Read a Column including if its nullable.
--
--   After we're done, increment the column index.
decodeColumn :: Monad m => Type -> OrcDecode m Column
decodeColumn typs =
  withPresence <*> decodeColumnPart typs <* incrementColumn


-- | Read a Column
--
--   The present column component has already been handled.
decodeColumnPart :: Monad m => Type -> OrcDecode m Column
decodeColumnPart typs = do
  currentEncoding' <-
     currentEncoding

  let
    encodingKind =
      columnEncodingKind currentEncoding'

  case (typs, encodingKind) of
    (BOOLEAN, _) -> do
      dataBytes <- popStream
      bits      <- liftEither (decodeBits dataBytes)
      return $ Bool bits

    (BYTE, _) -> do
      dataBytes      <- popStream
      bytes          <- liftEither (decodeBytes dataBytes)
      return $ Bytes bytes

    (SHORT, enc) -> do
      dataBytes <- popStream
      Short <$> liftEither (decodeIntegerRLEversion enc dataBytes)

    (INT, enc) -> do
      dataBytes <- popStream
      Integer <$> liftEither (decodeIntegerRLEversion enc dataBytes)

    (LONG, enc) -> do
      dataBytes <- popStream
      Long <$> liftEither (decodeIntegerRLEversion enc dataBytes)

    (FLOAT, _) -> do
      dataBytes <- popStream
      floats <- liftEither (decodeFloat32 dataBytes)
      return $ Float floats

    (DOUBLE, _) -> do
      dataBytes <- popStream
      doubles   <- liftEither (decodeFloat64 dataBytes)
      return $ Double doubles

    (STRING, encoding) ->
      String <$> decodeString encoding

    (CHAR, encoding) ->
      Char <$> decodeString encoding

    (VARCHAR, encoding) ->
      VarChar <$> decodeString encoding

    (DECIMAL, enc) -> do
      dataBytes  <- popStream
      scaleBytes <- popStream
      words      <- liftEither (decodeBase128Varint dataBytes)
      scale      <- liftEither (decodeIntegerRLEversion enc scaleBytes)

      return $ Decimal words scale

    (TIMESTAMP, enc) -> do
      secondsBytes <- popStream
      _nanoBytes   <- popStream
      Timestamp <$> liftEither (decodeIntegerRLEversion enc secondsBytes)

    (DATE, enc) -> do
      dataBytes <- popStream
      Date <$> liftEither (decodeIntegerRLEversion enc dataBytes)

    (BINARY, encoding) ->
      Binary <$> decodeString encoding

    (STRUCT fields, _) ->
      nestedColumn $
        decodeStruct fields

    (UNION fields, _) -> do
      tagBytes <-
        popStream

      tags <-
        liftEither $
          decodeBytes tagBytes

      nestedColumn $ do
        decodedFields <-
          fmap Boxed.fromList $
            for fields decodeColumn

        pure $
          Union tags decodedFields

    (LIST typ, enc) -> do
      lengthBytes <-
        popStream

      lengths <-
        liftEither (decodeIntegerRLEversion enc lengthBytes)

      nestedColumn $ do
        internal <-
          decodeColumn typ

        pure $
          List lengths internal

    (MAP keyTyp valTyp, enc) -> do
      lengthBytes <-
        popStream

      lengths <-
        liftEither (decodeIntegerRLEversion enc lengthBytes)

      nestedColumn $ do
        keys <-
          decodeColumn keyTyp

        values <-
          decodeColumn valTyp

        pure $
          Map lengths keys values

decodeString :: Monad m => Orc.ColumnEncodingKind -> OrcDecode m (Boxed.Vector ByteString)
decodeString = \case
  DIRECT ->
    decodeStringDirect decodeIntegerRLEv1

  DICTIONARY ->
    decodeStringDictionary decodeIntegerRLEv1

  DIRECT_V2 ->
    decodeStringDirect decodeIntegerRLEv2

  DICTIONARY_V2 ->
    decodeStringDictionary decodeIntegerRLEv2


decodeIntegerRLEversion :: (Storable.Storable w, OrcNum w) => Orc.ColumnEncodingKind -> ByteString ->  Either String (Storable.Vector w)
decodeIntegerRLEversion = \case
  DIRECT ->
    decodeIntegerRLEv1

  DICTIONARY ->
    decodeIntegerRLEv1

  DIRECT_V2 ->
    decodeIntegerRLEv2

  DICTIONARY_V2 ->
    decodeIntegerRLEv2


decodeStringDirect :: Monad m => (ByteString -> Either String (Storable.Vector Word64)) -> OrcDecode m (Boxed.Vector ByteString)
decodeStringDirect decodeIntegerFunc = do
  dataBytes   <- popStream
  lengthBytes <- popStream
  lengths <-
    liftEither (decodeIntegerFunc lengthBytes)

  return $!
    bytesOfSegmented $!
      splitByteString lengths dataBytes


decodeStringDictionary :: Monad m => (ByteString -> Either String (Storable.Vector Word64)) -> OrcDecode m (Boxed.Vector ByteString)
decodeStringDictionary decodeIntegerFunc = do
  dataBytes       <- popStream
  -- Specification appears to be incorrect here
  -- Length bytes comes before dictionary bytes;
  -- and, despite these being mandatory in the spec,
  -- there are files which do not have these streams.
  lengthBytes     <- popStream <|> pure ByteString.empty
  dictionaryBytes <- popStream <|> pure ByteString.empty

  selections :: Storable.Vector Word64 <-
    liftEither (decodeIntegerFunc dataBytes)

  lengths :: Storable.Vector Word64 <-
    liftEither (decodeIntegerFunc lengthBytes)

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
  fmap (Struct . Boxed.fromList) $
    for fields $
      traverse decodeColumn


liftMaybe :: MonadError e m => e -> Maybe a -> m a
liftMaybe =
  liftEither ... note


(...) :: (c -> d) -> (a -> b -> c) -> a -> b -> d
(...) = (.) . (.)


note :: a -> Maybe b -> Either a b
note x = maybe (Left x) Right
