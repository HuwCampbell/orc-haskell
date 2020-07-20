{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE FlexibleContexts    #-}

module Orc.Serial.Binary.Striped (
    withOrcFile
  , withOrcStripes
  , checkOrcFile

  , withFileLifted

  , putColumn
  , putOrcFile
) where

import           Control.Monad.IO.Class
import           Control.Monad.Except (MonadError, liftEither, throwError)
import           Control.Monad.State (MonadState (..), StateT (..), evalStateT, modify')
import           Control.Monad.Reader (ReaderT (..), runReaderT, ask)
import           Control.Monad.Trans.Control (MonadTransControl (..))
import           Control.Monad.Trans.Class (MonadTrans (..))
import           Control.Monad.Trans.Either (EitherT, newEitherT, left)

import           Data.Serialize.Put (PutM)
import qualified Data.Serialize.Put as Put
import qualified Data.Serialize.Get as Get

import           Data.List (dropWhile, reverse)

import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import           Data.Word (Word32)

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Streaming.Prelude as Streaming
import qualified Streaming.Internal as Streaming
import qualified Data.ByteString.Streaming as ByteStream
import qualified Data.ByteString.Streaming.Internal as ByteStream

import           Orc.Data.Segmented
import           Orc.Data.Data (StructField (..), Indexed, currentIndex, currentValue, nextIndex, makeIndexed, prevIndex)
import           Orc.Schema.Types as Orc

import           Orc.Serial.Protobuf.Schema as Orc
import           Orc.Serial.Binary.Internal.Bytes
import           Orc.Serial.Binary.Internal.Compression
import           Orc.Serial.Binary.Internal.Integers
import           Orc.Serial.Binary.Internal.OrcNum

import           Orc.Table.Striped (Column (..))
import qualified Orc.Table.Striped as Striped

import           Orc.X.Vector

import           System.IO as IO

import           Orc.Prelude

type ByteStream = ByteStream.ByteString


withFileLifted
  :: (Monad (t IO), MonadTransControl t)
  => FilePath
  -> IOMode
  -> (Handle -> t IO r)
  -> t IO r
withFileLifted file mode action =
  liftWith (\run -> withFile file mode (run . action)) >>=
    restoreT . return


withOrcFile :: FilePath -> ((Handle, PostScript, Footer) -> EitherT String IO r) -> EitherT String IO r
withOrcFile file action =
  withFileLifted file ReadMode $ \handle -> do
    (postScript, footer) <-
      checkMagic handle

    action (handle, postScript, footer)


checkOrcFile :: FilePath -> EitherT String IO ([StripeInformation], Type, Maybe CompressionKind)
checkOrcFile file =
  withOrcFile file $ \(_, postScript, footer) -> do
    let
      stripeInfos =
        stripes footer

      typeInfo =
        types footer

    return (stripeInfos, typeInfo, compression postScript)


withOrcStripes
  :: MonadIO m
  => FilePath
  -> ((Streaming.Stream (Of (StripeInformation, Column)) (EitherT String m) ()) -> EitherT String IO r)
  -> EitherT String IO r
withOrcStripes file action =
  withOrcFile file $ \(handle, postScript, footer) -> do
    let
      stripeInfos =
        stripes footer

      typeInfo =
        types footer

    action $
      Streaming.mapM
        (readStripe typeInfo (compression postScript) handle)
        (Streaming.each stripeInfos)



-- | Checks that the magic values are present in the file
--   (i.e. makes sure that it is actually an ORC file)
checkMagic :: MonadIO m => Handle -> EitherT String m (PostScript, Footer)
checkMagic handle = do
  liftIO  $ hSeek handle AbsoluteSeek 0
  header <- liftIO (ByteString.hGet handle 3)

  unless (header == "ORC") $
    left "Invalid header - probably not an ORC file."

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

    _:_ ->
      throwError $
        "Streams remain, but not for column: " <> show (currentIndex ix)

    _ ->
      throwError $
        "No streams remaining trying to pop column: " <> show (currentIndex ix)


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
      dataBytes <- popStream
      bytes     <- liftEither (decodeBytes dataBytes)
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
      floats    <- liftEither (decodeFloat32 dataBytes)
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
      nanoBytes    <- popStream

      seconds      <- liftEither (decodeIntegerRLEversion enc secondsBytes)
      nanos        <- liftEither (decodeIntegerRLEversion enc nanoBytes)

      return $ Timestamp seconds (Storable.map parseNano nanos)

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

      nestedColumn $
        Union tags . Boxed.fromList
          <$> for fields decodeColumn

    (LIST typ, enc) -> do
      lengthBytes <-
        popStream

      lengths <-
        liftEither (decodeIntegerRLEversion enc lengthBytes)

      nestedColumn $
        List lengths
          <$> decodeColumn typ

    (MAP keyTyp valTyp, enc) -> do
      lengthBytes <-
        popStream

      lengths <-
        liftEither (decodeIntegerRLEversion enc lengthBytes)

      nestedColumn $
        Map lengths
          <$> decodeColumn keyTyp
          <*> decodeColumn valTyp


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
      Boxed.map (\i -> dictionary Boxed.! (fromIntegral i)) $
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


-- * -------------------
-- Encoding
-- * -------------------



putOrcFile :: (MonadTransControl t, MonadIO (t IO), MonadError String (t IO)) => FilePath -> Streaming.Stream (Of Column) (t IO) () -> t IO ()
putOrcFile file column =
  withFileLifted file WriteMode $ \handle -> do
    ByteStream.toHandle handle $
      putOrcStream column



putOrcStream :: (MonadError String m, MonadIO m) => Streaming.Stream (Of Column) m () -> ByteStream m ()
putOrcStream column = do
  "ORC"

  (len, stripeInfos, t) :> () <-
    hyloByteStream putStripe (3,[],BOOLEAN) column

  footerLen :> () <-
    stream_ $ do
      putFooter $
        Footer
          (Just 3)
          (Just (len + 3))
          (reverse stripeInfos)
          t
          []
          (Nothing)
          []
          Nothing

  psLength :> () <-
    stream_ $
      putPostScript $
        PostScript
          (fromIntegral footerLen)
          (Just NONE)
          Nothing
          [0,12]
          Nothing
          (Just "ORC")

  untied $
    Put.putWord8 $
      fromIntegral psLength


type StripeState = (Word32, [Orc.ColumnEncoding], [Orc.Stream])


putStripe :: (MonadError String m, MonadIO m) => (Word64, [StripeInformation], Type) -> Column -> ByteStream m (Word64, [StripeInformation], Type)
putStripe (start, sis, _) column = do
  numRows <- lift $ liftEither $ Striped.length column

  (lenD :> (typ,(_,e,s))) <-
    streamingLength $
      flip runStateT (0,[],[]) $ do
        ByteStream.distribute $
          putColumn column

  (lenF :> ()) <-
    stream_ $
      putStripeFooter $ StripeFooter (reverse s) (reverse e) Nothing

  let
    si =
      StripeInformation
        (Just start)
        (Just 0)
        (Just lenD)
        (Just lenF)
        (Just (fromIntegral numRows))

  return (start + lenD + lenF, si : sis, typ)


incColumn :: Monad m => StateT StripeState m ()
incColumn =
  modify' $ \(ix, enc, streams_) ->
    (ix + 1, enc, streams_)


decColumn :: Monad m => StateT StripeState m ()
decColumn =
  modify' $ \(ix, enc, streams_) ->
    (ix - 1, enc, streams_)


nestedEncode :: Monad m => ByteStream (StateT StripeState m) r -> ByteStream (StateT StripeState m) r
nestedEncode act =
  lift incColumn *> act <* lift decColumn


record :: Monad m => (Word32 -> Orc.Stream) -> ByteStream (StateT StripeState m) ()
record stream =
  lift $
    modify' $ \(ix, enc, streams_) ->
      (ix, enc, stream ix : streams_)


fullEncoding :: Monad m => Orc.ColumnEncoding -> ByteStream (StateT StripeState m) ()
fullEncoding colEnc =
  lift $
    modify' $ \(ix, enc, streams_) ->
      (ix, colEnc : enc, streams_)


simpleEncoding :: Monad m => Orc.ColumnEncodingKind -> ByteStream (StateT StripeState m) ()
simpleEncoding colEncKind =
  fullEncoding (Orc.ColumnEncoding colEncKind Nothing)


putColumn :: MonadIO m => Column -> ByteStream (StateT StripeState m) Type
putColumn col =
  putColumnPart col <* lift incColumn


putColumnPart :: MonadIO m => Column -> ByteStream (StateT StripeState m) Type
putColumnPart = \case
  Bool bits   -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putBits bits
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l))
    return BOOLEAN


  Bytes bytes -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putBytes bytes
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l))
    return BYTE

  Short shorts -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putIntegerRLEv1 shorts
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l))
    return SHORT

  Integer ints -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putIntegerRLEv1 ints
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l))
    return INT

  Long longs -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putIntegerRLEv1 longs
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l))
    return LONG

  Float floats -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putFloat32 floats
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l))
    return FLOAT

  Double doubles -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putFloat64 doubles
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l))
    return DOUBLE

  String strs -> do
    putStringColumn strs
    return STRING

  Char strs -> do
    putStringColumn strs
    return CHAR

  VarChar strs -> do
    putStringColumn strs
    return VARCHAR

  Binary strs -> do
    putStringColumn strs
    return BINARY

  Decimal words scale -> do
    _          <- simpleEncoding DIRECT
    (l0 :> _)  <- stream_ $ Storable.mapM putBase128Varint words
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l0))
    (l1 :> _)  <- stream_ $ putIntegerRLEv1 scale
    _          <- record (\ix -> Stream (Just SK_SECONDARY) (Just ix) (Just l1))
    return DECIMAL


  Timestamp seconds nanos -> do
    _          <- simpleEncoding DIRECT
    (l0 :> _)  <- stream_ $ putIntegerRLEv1 seconds
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l0))
    (l1 :> _)  <- stream_ $ putIntegerRLEv1 (Storable.map lazyNano nanos)
    _          <- record (\ix -> Stream (Just SK_SECONDARY) (Just ix) (Just l1))
    return DECIMAL


  Date dates -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putIntegerRLEv1 dates
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l))
    return DATE


  Struct fields -> do
    _          <- simpleEncoding DIRECT
    nestedType <-
      nestedEncode $
        traverse (traverse putColumn) fields

    return $ STRUCT (toList nestedType)


  List lengths nested -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putIntegerRLEv1 lengths
    _          <- record (\ix -> Stream (Just SK_LENGTH) (Just ix) (Just l))
    nestedType <-
      nestedEncode $
        putColumn nested

    return $ LIST nestedType


  Map lengths keys values -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putIntegerRLEv1 lengths
    _          <- record (\ix -> Stream (Just SK_LENGTH) (Just ix) (Just l))
    (kT, vT)   <-
      nestedEncode $
        (,) <$> putColumn keys
            <*> putColumn values

    return $ MAP kT vT


  Union tags inners -> do
    _          <- simpleEncoding DIRECT
    (l :> _)   <- stream_ $ putBytes tags
    _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l))
    innerTypes <-
      nestedEncode $
        for inners putColumn

    return $ UNION (toList innerTypes)


  Partial presence inner -> do
    (l :> _)   <- stream_ $ putBits presence
    _          <- record (\ix -> Stream (Just SK_PRESENT) (Just ix) (Just l))
    putColumnPart inner


putStringColumn :: MonadIO m => Boxed.Vector ByteString -> ByteStream (StateT StripeState m) ()
putStringColumn strs = do
  _          <- simpleEncoding DIRECT
  (l0 :> _)  <- stream_ $ for strs Put.putByteString
  _          <- record (\ix -> Stream (Just SK_DATA) (Just ix) (Just l0))

  (l1 :> _)  <- stream_ $ putIntegerRLEv1 (Storable.convert $ fmap (i2w32 . ByteString.length) strs)
  _          <- record (\ix -> Stream (Just SK_LENGTH) (Just ix) (Just l1))
  return ()


stream_ :: MonadIO m => PutM a -> ByteStream m (Of Word64 a)
stream_ = streamingLength . untied
{-# INLINE stream_ #-}


untied :: MonadIO m => PutM a -> ByteStream m a
untied x =
  let (a, bldr) = Put.runPutMBuilder x
  in  ByteStream.toStreamingByteString bldr $> a
{-# INLINE untied #-}


i2w32 :: Int -> Word32
i2w32 = fromIntegral
{-# INLINE i2w32 #-}


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
