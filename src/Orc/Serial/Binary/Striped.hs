{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE FlexibleContexts    #-}

module Orc.Serial.Binary.Striped (
    withOrcFile
  , putOrcFile

  -- * Internal
  , readStripe
  , putOrcStream
) where

import           Control.Monad.IO.Class
import           Control.Monad.Except (MonadError, liftEither, throwError)
import           Control.Monad.State.Strict (MonadState (..), StateT (..), evalStateT, runState, modify')
import           Control.Monad.Reader (MonadReader, ReaderT (..), runReaderT, ask)
import           Control.Monad.Trans.Class (MonadTrans (..))
import           Control.Monad.Trans.Either (EitherT)

import           Data.Serialize.Put (PutM)
import qualified Data.Serialize.Put as Put

import           Data.List (dropWhile, reverse, sort)
import qualified Data.Map.Strict as Map
import qualified Data.Tuple as Tuple

import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable
import           Data.Word (Word8, Word32)

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Streaming.Prelude as Streaming
import qualified Data.ByteString.Streaming as ByteStream

import           Orc.Data.Segmented
import           Orc.Data.Data (StructField (..), Indexed, currentIndex, currentValue, nextIndex, makeIndexed, prevIndex)
import           Orc.Schema.Types as Orc

import           Orc.Serial.Protobuf.Schema as Orc
import           Orc.Serial.Binary.Internal.Bytes
import           Orc.Serial.Binary.Internal.Compression
import           Orc.Serial.Binary.Internal.Integers
import           Orc.Serial.Binary.Internal.OrcNum

import           Orc.Serial.Binary.Base (MonadTransIO)
import qualified Orc.Serial.Binary.Base as Base

import           Orc.Table.Striped (Column (..))
import qualified Orc.Table.Striped as Striped

import           Orc.X.Streaming

import           System.IO as IO

import           Orc.Prelude

type ByteStream = ByteStream.ByteString


withOrcFile
  :: MonadTransIO t
  => FilePath
  -> (Type -> (Streaming.Stream (Of (StripeInformation, Column)) (t IO) ()) -> t IO r)
  -> t IO r
withOrcFile file action =
  Base.withOrcFile file $ \(handle, postScript, footer) -> do
    let
      stripeInfos =
        stripes footer

      typeInfo =
        types footer

    action typeInfo $
      Streaming.mapM
        (readStripe typeInfo (compression postScript) handle)
        (Streaming.each stripeInfos)


{-# SPECIALIZE
  withOrcFile
    :: FilePath
    -> (Type -> (Streaming.Stream (Of (StripeInformation, Column)) (EitherT String IO) ())
    -> EitherT String IO r)
    -> EitherT String IO r #-}


readStripe
  :: MonadTransIO t
  => Type
  -> Maybe CompressionKind
  -> Handle
  -> StripeInformation
  -> t IO (StripeInformation, Column)
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
    liftMaybe "Required Field Missing - Index Length"
      (indexLength stripeInfo)

  rdLength <-
    liftMaybe "Required Field Missing - Data Length"
      (dataLength stripeInfo)

  rsLength <-
    liftMaybe "Required Field Missing - Footer length"
      (siFooterLength stripeInfo)

  numRows <-
    liftMaybe "Required Field Missing - Number of Rows"
      (siNumberOfRows stripeInfo)

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
    liftEither . readCompressedStream mCompressionInfo =<< liftIO (ByteString.hGet handle $ fromIntegral rsLength)

  stripeFooter <-
    liftEither $
      readStripeFooter stripeFooterData

  let
    nonRowIndexStreams =
      dropWhile (\s -> streamKind s == Just SK_ROW_INDEX || streamKind s == Just SK_BLOOM_FILTER) $
        streams stripeFooter

    columnsEncodings =
      columns stripeFooter

  --
  -- Jump back to dataBytes
  liftIO $
    hSeek handle AbsoluteSeek dataBytesStart

  column <-
    decodeTable numRows typeInfo mCompressionInfo columnsEncodings nonRowIndexStreams $
      ByteStream.hGet handle $ fromIntegral rdLength

  return (stripeInfo, column)


decodeTable
  :: MonadTransIO t
  => Word64
  -> Type
  -> Maybe CompressionKind
  -> [Orc.ColumnEncoding]
  -> [Orc.Stream]
  -> ByteStream IO ()
  -> t IO Column
decodeTable rows typs mCompression encodings orcStreams dataBytes =
  evalStateT (runReaderT (decodeColumn typs rows) mCompression) (makeIndexed encodings, orcStreams, dataBytes)


type OrcDecode t m = ReaderT (Maybe CompressionKind) (StateT (Indexed Orc.ColumnEncoding, [Orc.Stream], ByteStream m ()) (t m))


withPresence
  :: MonadTransIO t
  => Word64 -> (Word64 -> OrcDecode t IO Column) -> OrcDecode t IO Column
withPresence rows act = do
  mPresenceBytes <- popOptionalStream SK_PRESENT
  case mPresenceBytes of
    Just presenceBytes -> do
      presenceColumn <-
        liftEither $
          Storable.take (fromIntegral rows) <$>
            decodeBits presenceBytes

      let
        innerRows =
          fromIntegral . Storable.length $
            Storable.filter id presenceColumn

      Partial presenceColumn <$> act innerRows

    Nothing ->
      act rows


popOptionalStream :: MonadTransIO t => StreamKind -> OrcDecode t IO (Maybe ByteString)
popOptionalStream sk = do
  (ix, streams0, _bytes) <- get
  case streams0 of
    s:_ | streamColumn s == Just (currentIndex ix) && streamKind s == Just sk -> do
      Just <$> popStream
    _else ->
      return Nothing


popStream :: MonadTransIO t => OrcDecode t IO ByteString
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


incrementColumn :: Monad (t m) => OrcDecode t m ()
incrementColumn =
  modify' $ \(ix, a, b) -> (nextIndex ix, a, b)


decrementColumn :: Monad (t m) => OrcDecode t m ()
decrementColumn =
  modify' $ \(ix, a, b) -> (prevIndex ix, a, b)


-- | Push inwards when running a nested column
--   such as a Struct, Map or List.
--   We need to pop out again, as the wrapping decodeColumn
--   will increment the column when complete.
nestedColumn :: Monad (t m) => OrcDecode t m a -> OrcDecode t m a
nestedColumn f =
  incrementColumn *> f <* decrementColumn


currentEncoding :: (MonadError String (t m), Monad (t m)) => OrcDecode t m (Orc.ColumnEncoding)
currentEncoding = do
  (ix, _, _) <- get
  maybe
    (throwError $ "Couldn't find Column encoding for column: " <> show ix)
    pure
    (currentValue ix)


-- | Read a Column including if it's nullable.
--
--   After we're done, increment the column index.
decodeColumn :: MonadTransIO t => Type -> Word64 -> OrcDecode t IO Column
decodeColumn typs rows =
  withPresence rows (decodeColumnPart typs) <* incrementColumn


-- | Read a Column
--
--   The present column component has already been handled.
decodeColumnPart :: MonadTransIO t => Type -> Word64 -> OrcDecode t IO Column
decodeColumnPart typs rows = do
  currentEncoding' <-
     currentEncoding

  let
    encodingKind =
      columnEncodingKind currentEncoding'

  case (typs, encodingKind) of
    (BOOLEAN, _) -> do
      dataBytes <-
        popStream
      bits <-
        Storable.take (fromIntegral rows) <$>
          liftEither (decodeBits dataBytes)

      return $ Bool bits

    (BYTE, _) -> do
      dataBytes <- popStream
      bytes     <- liftEither (decodeBytes dataBytes)
      return $ Byte bytes

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

      return $ Timestamp seconds (Storable.map decodeNanoseconds nanos)

    (DATE, enc) -> do
      dataBytes <- popStream
      Date <$> liftEither (decodeIntegerRLEversion enc dataBytes)

    (BINARY, encoding) ->
      Binary <$> decodeString encoding

    (STRUCT fields, _) ->
      nestedColumn $
        decodeStruct rows fields

    (UNION fields, _) -> do
      tagBytes <-
        popStream

      tags <-
        liftEither $
          decodeBytes tagBytes

      nestedColumn $
        Union tags . Boxed.fromList
          <$> ifor fields (\i f -> decodeColumn f (tagsRows i tags))

    (LIST typ, enc) -> do
      lengthBytes <-
        popStream

      lengths <-
        liftEither (decodeIntegerRLEversion enc lengthBytes)

      let
        nestedRows = Storable.sum (Storable.map fromIntegral lengths)

      nestedColumn $
        List lengths
          <$> decodeColumn typ nestedRows

    (MAP keyTyp valTyp, enc) -> do
      lengthBytes <-
        popStream

      lengths <-
        liftEither (decodeIntegerRLEversion enc lengthBytes)

      let
        nestedRows = Storable.sum (Storable.map fromIntegral lengths)

      nestedColumn $
        Map lengths
          <$> decodeColumn keyTyp nestedRows
          <*> decodeColumn valTyp nestedRows


decodeString :: MonadTransIO t => Orc.ColumnEncodingKind -> OrcDecode t IO (Boxed.Vector ByteString)
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


decodeStringDirect :: MonadTransIO t => (ByteString -> Either String (Storable.Vector Word64)) -> OrcDecode t IO (Boxed.Vector ByteString)
decodeStringDirect decodeIntegerFunc = do
  dataBytes   <- popStream
  lengthBytes <- popStream
  lengths <-
    liftEither (decodeIntegerFunc lengthBytes)

  return $!
    bytesOfSegmented $!
      splitByteString lengths dataBytes


decodeStringDictionary :: MonadTransIO t => (ByteString -> Either String (Storable.Vector Word64)) -> OrcDecode t IO (Boxed.Vector ByteString)
decodeStringDictionary decodeIntegerFunc = do
  dataBytes        <- popStream
  -- Specification appears to be incorrect here
  -- Length bytes comes before dictionary bytes;
  -- and, despite these being mandatory in the spec,
  -- there are files which do not have these streams.
  mLengthBytes     <- popOptionalStream SK_LENGTH
  mDictionaryBytes <- popOptionalStream SK_DICTIONARY_DATA

  selections <-
    liftEither (decodeIntegerFunc dataBytes)

  lengths <-
    liftEither (decodeIntegerFunc (fromMaybe ByteString.empty mLengthBytes))

  let
    dictionary =
      Boxed.convert . bytesOfSegmented $
        splitByteString lengths (fromMaybe ByteString.empty mDictionaryBytes)

    discovered =
      Boxed.map (\i -> dictionary Boxed.! (fromIntegral i)) $
        Boxed.convert selections

  return $
    discovered


decodeStruct :: MonadTransIO t => Word64 -> [StructField Type] -> OrcDecode t IO Column
decodeStruct rows fields =
  fmap (Struct . Boxed.fromList) $
    for fields $
      traverse (decodeColumn ? rows)


tagsRows :: Int -> Storable.Vector Word8 -> Word64
tagsRows ix0 = do
  let
    ix = fromIntegral ix0

  fromIntegral
    . Storable.length
    . Storable.filter (== ix)



liftMaybe :: MonadError e m => e -> Maybe a -> m a
liftMaybe =
  liftEither ... note



-- * -------------------
-- Encoding
-- * -------------------


putOrcFile
  :: MonadTransIO t
  => Maybe Type
  -> Maybe CompressionKind
  -> FilePath
  -> Streaming.Stream (Of Column) (t IO) ()
  -> t IO ()
putOrcFile expectedType mCmprssn file column =
  Base.withBinaryFileLifted file WriteMode $ \handle -> do
    runReaderT ? mCmprssn $
      ByteStream.toHandle handle $
        putOrcStream expectedType $
          Streaming.hoist lift column

{-# SPECIALIZE
  putOrcFile
    :: Maybe Type
    -> Maybe CompressionKind
    -> FilePath
    -> Streaming.Stream (Of Column) (EitherT String IO) ()
    -> EitherT String IO () #-}


putOrcStream :: (MonadReader (Maybe CompressionKind) m, MonadError String m, MonadIO m) => Maybe Type -> Streaming.Stream (Of Column) m () -> ByteStream m ()
putOrcStream expectedType tableStream = do
  "ORC"

  (len, stripeInfos, t) :> () <-
    hyloByteStream putTable (3,[],expectedType) tableStream

  ft <-
    lift $ maybe
      (throwError "No type information could be gathered from the stream and no default was given.")
      pure t

  footerLen :> () <-
    put_stream $ do
      putFooter $
        Footer
          (Just 3)
          (Just (len + 3))
          (reverse stripeInfos)
          ft
          []
          (Nothing)
          []
          Nothing

  cmprssn <- lift ask

  psLength :> () <-
    put_uncompressed_stream $
      putPostScript $
        PostScript
          (fromIntegral footerLen)
          cmprssn
          Nothing
          [0,12]
          Nothing
          (Just "ORC")

  streamingPut $
    Put.putWord8 $
      fromIntegral psLength


type StripeState = (Word32, [Orc.ColumnEncoding], [Orc.Stream])


putTable :: (MonadReader (Maybe CompressionKind) m, MonadError String m, MonadIO m) => (Word64, [StripeInformation], Maybe Type) -> Column -> ByteStream m (Word64, [StripeInformation], Maybe Type)
putTable (start, sis, startingType) column = do
  let numRows = Striped.length column

  (lenD :> (typ,(_,e,s))) <-
    streamingLength $
      runStateT ? (0,[],[]) $ do
        ByteStream.distribute $
          putColumn column

  for_ startingType $ \st ->
    unless (st == typ) $
      lift $
        throwError ("Type of stripe wasn't expected. Expected " <> show st <> ", Stripe: " <> show typ)

  (lenF :> ()) <-
    put_stream $
      putStripeFooter $ StripeFooter (reverse s) (reverse e) Nothing

  let
    si =
      StripeInformation
        (Just start)
        (Just 0)
        (Just lenD)
        (Just lenF)
        (Just (fromIntegral numRows))

  return (start + lenD + lenF, si : sis, Just typ)


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


putColumn :: (MonadReader (Maybe CompressionKind) m, MonadError String m, MonadIO m) => Column -> ByteStream (StateT StripeState m) Type
putColumn col =
  putColumnPart col <* lift incColumn


putColumnPart :: (MonadReader (Maybe CompressionKind) m, MonadError String m, MonadIO m) => Column -> ByteStream (StateT StripeState m) Type
putColumnPart = \case
  Bool bits   -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putBits bits
    return BOOLEAN

  Byte bytes -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putBytes bytes
    return BYTE

  Short shorts -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putIntegerRLEv1 shorts
    return SHORT

  Integer ints -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putIntegerRLEv1 ints
    return INT

  Long longs -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putIntegerRLEv1 longs
    return LONG

  Float floats -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putFloat32 floats
    return FLOAT

  Double doubles -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putFloat64 doubles
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
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ Storable.mapM_ putBase128Varint words
    put_data_stream SK_SECONDARY $ putIntegerRLEv1 scale
    return DECIMAL

  Timestamp seconds nanos -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putIntegerRLEv1 seconds
    put_data_stream SK_SECONDARY $ putIntegerRLEv1 (Storable.map encodeNanoseconds nanos)
    return TIMESTAMP

  Date dates -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putIntegerRLEv1 dates
    return DATE

  Struct fields -> do
    simpleEncoding DIRECT
    nestedType <-
      nestedEncode $
        traverse (traverse putColumn) fields

    return $ STRUCT (toList nestedType)


  List lengths nested -> do
    simpleEncoding DIRECT
    put_data_stream SK_LENGTH $ putIntegerRLEv1 lengths
    nestedType <-
      nestedEncode $
        putColumn nested

    return $ LIST nestedType


  Map lengths keys values -> do
    simpleEncoding DIRECT
    put_data_stream SK_LENGTH $ putIntegerRLEv1 lengths

    (kT, vT)   <-
      nestedEncode $
        (,) <$> putColumn keys
            <*> putColumn values

    return $ MAP kT vT


  Union tags inners -> do
    simpleEncoding DIRECT
    put_data_stream SK_DATA $ putBytes tags
    innerTypes <-
      nestedEncode $
        for inners putColumn

    return $ UNION (toList innerTypes)


  Partial presence inner -> do
    put_data_stream SK_PRESENT $ putBits presence
    putColumnPart inner


putStringColumn
  :: (MonadReader (Maybe CompressionKind) m, MonadError String m, MonadIO m)
  => Boxed.Vector ByteString
  -> ByteStream (StateT StripeState m) ()
putStringColumn strs =
  let
    addOrFind str = do
      !(dict,cur) <- get
      let key = Map.lookup str dict
      case key of
        Just ix ->
          return ix
        Nothing -> do
          put (Map.insert str cur dict, succ cur)
          return cur

    (indicies, (dictMap, _)) =
      runState ? (Map.empty, 0) $ do
        mapM addOrFind strs

    dictVec =
      Boxed.fromList . fmap snd . sort . fmap Tuple.swap $
        Map.toList dictMap

    dictSize =
      fromIntegral $
        Map.size dictMap

    originalSize =
      fromIntegral $
        Boxed.length indicies

    threshold :: Double
    threshold =
      0.5

  in
    if (dictSize < originalSize * threshold) then
      putDictionaryColumn (Storable.convert indicies) dictVec
    else
      putDirectColumn strs


putDirectColumn
  :: (MonadReader (Maybe CompressionKind) m, MonadError String m, MonadIO m)
  => Boxed.Vector ByteString
  -> ByteStream (StateT StripeState m) ()
putDirectColumn strs = do
  simpleEncoding DIRECT
  put_data_stream SK_DATA $ for_ strs Put.putByteString
  put_data_stream SK_LENGTH $ putIntegerRLEv1 (Storable.convert $ fmap (i2w32 . ByteString.length) strs)



putDictionaryColumn
  :: (MonadReader (Maybe CompressionKind) m, MonadError String m, MonadIO m)
  => Storable.Vector Word64
  -> Boxed.Vector ByteString
  -> ByteStream (StateT StripeState m) ()
putDictionaryColumn indicies dictionary = do
  fullEncoding $ Orc.ColumnEncoding DICTIONARY (Just (i2w32 (Boxed.length dictionary)))
  put_data_stream SK_DATA $ putIntegerRLEv1 indicies
  put_data_stream SK_LENGTH $ putIntegerRLEv1 (Storable.convert $ fmap (i2w32 . ByteString.length) dictionary)
  put_data_stream SK_DICTIONARY_DATA $ for_ dictionary Put.putByteString


put_data_stream :: (MonadReader (Maybe CompressionKind) m, MonadError String m, MonadIO m) => StreamKind -> PutM a -> ByteStream (StateT StripeState m) a
put_data_stream sk p = do
  (len :> a)  <- put_stream p
  record (\ix -> Stream (Just sk) (Just ix) (Just len))
  return a


put_stream :: (MonadReader (Maybe CompressionKind) m, MonadError String m, MonadIO m) => PutM a -> ByteStream m (Of Word64 a)
put_stream p = do
  streamingLength . ByteStream.mwrap $ do
    cmprssn     <- ask
    strict :> a <- ByteStream.toStrict (streamingPut p )
    blah        <- liftEither $ writeCompressedStream cmprssn strict
    return $ ByteStream.toStreamingByteString blah $> a


put_uncompressed_stream :: MonadIO m => PutM a -> ByteStream m (Of Word64 a)
put_uncompressed_stream = streamingLength . streamingPut


i2w32 :: Int -> Word32
i2w32 = fromIntegral
{-# INLINE i2w32 #-}

(?) :: (a -> b -> c) -> b -> a -> c
(?) = flip
{-# INLINE (?) #-}
