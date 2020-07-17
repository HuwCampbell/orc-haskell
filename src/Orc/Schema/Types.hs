{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds     #-}
{-# LANGUAGE LambdaCase    #-}

module Orc.Schema.Types (
    CompressionKind (..)
  , StreamKind (..)
  , ColumnEncodingKind (..)
  , Type (..)
  , StripeInformation (..)
  , ColumnEncoding (..)
  , Stream (..)

  , PostScript (..)
  , readPostScript
  , putPostScript

  , Footer (..)
  , readFooter
  , putFooter

  , StripeFooter (..)
  , readStripeFooter
  , putStripeFooter

  , RowIndexEntry (..)
  , RowIndex (..)
  , readRowIndex
  , putRowIndex


  -- * Low level protobuffer transformations
  , fromProtoFooter
  , toProtoFooter

  , toProtoTypes
  , fromProtoTypes

  , fromProtoStream
  , toProtoStream

  , fromProtoPostScript
  , toProtoPostScript
  ) where

import           Data.Int
import           Data.ByteString (ByteString)
import           Data.Foldable (foldl')
import           Data.ProtocolBuffers
import           Data.Text (Text)
import           Data.Word
import           GHC.Generics (Generic)
import           Data.Traversable (for)

import           Control.Monad.Trans.State (state, runState)

import qualified Data.Serialize.Get as Get
import qualified Data.Serialize.Put as Put

import           Orc.Data.Data (StructField (..), StructFieldName (..))
import qualified Orc.Schema.Protobuf.Definitions as Proto

data CompressionKind
  = NONE
  | ZLIB
  | SNAPPY
  | LZO
  | LZ4
  | ZSTD
  deriving (Eq, Ord, Show, Enum)

fromProtoCompressionKind :: Proto.CompressionKind -> CompressionKind
fromProtoCompressionKind = \case
  Proto.NONE -> NONE
  Proto.ZLIB -> ZLIB
  Proto.SNAPPY -> SNAPPY
  Proto.LZO -> LZO
  Proto.LZ4 -> LZ4
  Proto.ZSTD -> ZSTD

toProtoCompressionKind :: CompressionKind -> Proto.CompressionKind
toProtoCompressionKind = \case
  NONE -> Proto.NONE
  ZLIB -> Proto.ZLIB
  SNAPPY -> Proto.SNAPPY
  LZO -> Proto.LZO
  LZ4 -> Proto.LZ4
  ZSTD -> Proto.ZSTD

data PostScript = PostScript {
    -- the length of the footer section in bytes
    -- NOTE: Changed to Required from Optional
    footerLength :: Word64
    -- the kind of generic compression used
  , compression ::  Maybe CompressionKind
    -- the maximum size of each compression chunk
  , compressionBlockSize :: Maybe Word64
    -- the version of the writer
  , version :: [Word32]
    -- the length of the metadata section in bytes
  , metadataLength :: Maybe Word64
    -- the fixed string "ORC"
  , magic :: Maybe Text
  } deriving (Eq, Ord, Show)


readPostScript :: ByteString -> Either String PostScript
readPostScript bytes =
  fromProtoPostScript <$> Get.runGet decodeMessage bytes


putPostScript :: Put.Putter PostScript
putPostScript =
  encodeMessage . toProtoPostScript


fromProtoPostScript :: Proto.PostScript -> PostScript
fromProtoPostScript raw =
  PostScript
    (getField $ Proto.footerLength raw)
    (fmap fromProtoCompressionKind $ getField $ Proto.compression raw)
    (getField $ Proto.compressionBlockSize raw)
    (getField $ Proto.version raw)
    (getField $ Proto.metadataLength raw)
    (getField $ Proto.magic raw)


toProtoPostScript :: PostScript -> Proto.PostScript
toProtoPostScript hydrated =
  Proto.PostScript
    (putField $ footerLength hydrated)
    (putField $ fmap toProtoCompressionKind $ compression hydrated)
    (putField $ compressionBlockSize hydrated)
    (putField $ version hydrated)
    (putField $ metadataLength hydrated)
    (putField $ magic hydrated)


data Footer = Footer {
    -- the length of the file header in bytes (always 3)
    headerLength :: Maybe Word64
    -- the length of the file header and body in bytes
  , contentLength :: Maybe Word64
    -- the information about the stripes
  , stripes :: [StripeInformation]
    -- the schema information
  , types :: Type
    -- the user metadata that was added
  , metadata :: [UserMetadataItem]
    -- the total number of rows in the file
  , numberOfRows :: Maybe Word64
    -- the statistics of each column across the file
  , statistics :: [ColumnStatistics]
      -- the maximum number of rows in each index entry
  , rowIndexStride :: Maybe Word32
  } deriving (Eq, Ord, Show)


readFooter :: ByteString -> Either String Footer
readFooter bytes =
  fromProtoFooter =<< Get.runGet decodeMessage bytes


putFooter :: Put.Putter Footer
putFooter =
  encodeMessage . toProtoFooter

fromProtoFooter :: Proto.Footer -> Either String Footer
fromProtoFooter raw =
  Footer
    (getField $ Proto.headerLength raw)
    (getField $ Proto.contentLength raw)
    (fmap fromStripeInformation . getField $ Proto.stripes raw)
    <$> (fromProtoTypes . getField $ Proto.types raw)
    <*> pure (fmap fromUserMetadataItem . getField $ Proto.metadata raw)
    <*> pure (getField $ Proto.numberOfRows raw)
    <*> pure (fmap fromColumnStatistics $ getField $ Proto.statistics raw)
    <*> pure (getField $ Proto.rowIndexStride raw)


toProtoFooter :: Footer -> Proto.Footer
toProtoFooter hydrated =
  Proto.Footer
    (putField $ headerLength hydrated)
    (putField $ contentLength hydrated)
    (putField $ fmap toStripeInformation $ stripes hydrated)
    (putField $ toProtoTypes $ types hydrated)
    (putField $ fmap toUserMetadataItem  $ metadata hydrated)
    (putField $ numberOfRows hydrated)
    (putField $ mempty)
    (putField $ rowIndexStride hydrated)


data StripeInformation = StripeInformation {
    -- the start of the stripe within the file
    offset :: Maybe Word64
    -- the length of the indexes in bytes
  , indexLength :: Maybe Word64
    -- the length of the data in bytes
  , dataLength :: Maybe Word64
    -- the length of the footer in bytes
  , siFooterLength :: Maybe Word64
    -- the number of rows in the stripe
  , siNumberOfRows :: Maybe Word64
  } deriving (Eq, Ord, Show)


fromStripeInformation :: Proto.StripeInformation -> StripeInformation
fromStripeInformation raw =
  StripeInformation
    (getField $ Proto.offset raw)
    (getField $ Proto.indexLength raw)
    (getField $ Proto.dataLength raw)
    (getField $ Proto.siFooterLength raw)
    (getField $ Proto.siNumberOfRows raw)


toStripeInformation :: StripeInformation -> Proto.StripeInformation
toStripeInformation raw =
  Proto.StripeInformation
    (putField $ offset raw)
    (putField $ indexLength raw)
    (putField $ dataLength raw)
    (putField $ siFooterLength raw)
    (putField $ siNumberOfRows raw)


data Type =
    BOOLEAN
  | BYTE
  | SHORT
  | INT
  | LONG
  | FLOAT
  | DOUBLE
  | STRING
  | BINARY
  | TIMESTAMP
  | LIST Type
  | MAP Type Type
  | STRUCT [StructField Type]
  | UNION [Type]
  | DECIMAL
  | DATE
  | VARCHAR
  | CHAR
  deriving (Show, Eq, Ord)


fromProtoTypes :: [Proto.Type] -> Either String Type
fromProtoTypes typs
  | null typs
  = Left "Can't parse no types to a type. This is probably a protobuf error"
  | otherwise
  = let
      (typ, leftovers) =
        fromTypesContinuation typs
    in
    if null leftovers then
      Right typ
    else
      Left $ "Leftovers! Coundn't parse " <> show typs <> "\n\n\nLeftovers: " <> show leftovers

    where

  fromTypesContinuation :: [Proto.Type] -> (Type, [Proto.Type])
  fromTypesContinuation [] =
    (BOOLEAN, [])
  fromTypesContinuation (t:ts) =
    case getField (Proto.kind t) of
      Proto.BOOLEAN ->
        (BOOLEAN, ts)
      Proto.BYTE ->
        (BYTE, ts)
      Proto.SHORT ->
        (SHORT, ts)
      Proto.INT ->
        (INT, ts)
      Proto.LONG ->
        (LONG, ts)
      Proto.FLOAT ->
        (FLOAT, ts)
      Proto.DOUBLE ->
        (DOUBLE, ts)
      Proto.STRING ->
        (STRING, ts)
      Proto.BINARY ->
        (BINARY, ts)
      Proto.TIMESTAMP ->
        (TIMESTAMP, ts)
      Proto.DECIMAL ->
        (DECIMAL, ts)
      Proto.DATE ->
        (DATE, ts)
      Proto.VARCHAR ->
        (VARCHAR, ts)
      Proto.CHAR ->
        (CHAR, ts)

      Proto.LIST ->
        let
          (tx, rest) =
            fromTypesContinuation ts
        in
          (LIST tx, rest)
      Proto.MAP ->
        let
          (kx, k'rest) =
            fromTypesContinuation ts
          (vx, v'rest) =
            fromTypesContinuation k'rest
        in
          (MAP kx vx, v'rest)
      Proto.STRUCT ->
        let
          fs =
            getField (Proto.fieldNames t)

          (fields, rest) =
            runState
              (for fs $ \f -> StructField (StructFieldName f) <$> state fromTypesContinuation)
              ts
        in
          (STRUCT fields, rest)
      Proto.UNION ->
        let
          us =
            getField (Proto.subtypes t)

          (fields, rest) =
            runState
              (for us $ \_ -> state fromTypesContinuation)
              ts
        in
          (UNION fields, rest)


toProtoTypes :: Type -> [Proto.Type]
toProtoTypes hydrated =
  fst $ go hydrated 0
    where
  simple k =
    Proto.Type {
      Proto.kind = putField k
    , Proto.subtypes = mempty
    , Proto.fieldNames = mempty
    , Proto.maximumLength = mempty
    , Proto.precision = mempty
    , Proto.scale = mempty
    }

  withSubs k ss =
    (simple k) {
      Proto.subtypes = putField ss
    }

  withSubsFields k ss fs =
    (withSubs k ss) {
      Proto.fieldNames = putField (fmap getFieldName fs)
    }

  go h n = case h of
    BOOLEAN ->
      ([simple Proto.BOOLEAN], n + 1)
    BYTE ->
      ([simple Proto.BYTE], n + 1)
    SHORT ->
      ([simple Proto.SHORT], n + 1)
    INT ->
      ([simple Proto.INT], n + 1)
    LONG ->
      ([simple Proto.LONG], n + 1)
    FLOAT ->
      ([simple Proto.FLOAT], n + 1)
    DOUBLE ->
      ([simple Proto.DOUBLE], n + 1)
    STRING ->
      ([simple Proto.STRING], n + 1)
    BINARY ->
      ([simple Proto.BINARY], n + 1)
    TIMESTAMP ->
      ([simple Proto.TIMESTAMP], n + 1)
    DECIMAL ->
      ([simple Proto.DECIMAL], n + 1)
    DATE ->
      ([simple Proto.DATE], n + 1)
    VARCHAR ->
      ([simple Proto.VARCHAR], n + 1)
    CHAR ->
      ([simple Proto.CHAR], n + 1)
    LIST inner ->
      let (inner1, ix) = go inner (n + 1)
      in  (withSubs Proto.LIST [n+1] : inner1, ix)
    MAP k0 v0 ->
      let (k1, kn) = go k0 (n+1)
          (v1, vx) = go v0 kn
      in  (withSubs Proto.MAP [n+1,kn] : k1 <> v1, vx)
    STRUCT fs ->
      let step ((children0, acc), s) f =
            let (f1, ix) = go (fieldValue f) s
            in  ((children0 <> [fmap (const s) f], acc <> f1), ix)
          ((children, k1), kn) = foldl' step (([],[]), n + 1) fs
      in  (withSubsFields Proto.STRUCT (fmap fieldValue children) (fmap fieldName children) : k1, kn)
    UNION alts ->
      let step ((children0, acc), s) f =
            let (f1, ix) = go f s
            in  ((children0 <> [s], acc <> f1), ix)
          ((children, k1), kn) = foldl' step (([],[]), n + 1) alts
      in  (withSubs Proto.UNION children : k1, kn)

data ColumnStatistics = ColumnStatistics {
 -- the number of values
   numberOfValues :: Maybe Word64
 -- At most one of these has a value for any column
 , intStatistics :: Maybe Proto.IntegerStatistics
 , doubleStatistics :: Maybe Proto.DoubleStatistics
 , stringStatistics :: Maybe Proto.StringStatistics
 , bucketStatistics :: Maybe Proto.BucketStatistics
 , decimalStatistics :: Maybe Proto.DecimalStatistics
 , dateStatistics :: Maybe Proto.DateStatistics
 , binaryStatistics :: Maybe Proto.BinaryStatistics
 , timestampStatistics :: Maybe Proto.TimestampStatistics
 , hasNull :: Maybe Bool
} deriving (Eq, Ord, Show, Generic)


fromColumnStatistics :: Proto.ColumnStatistics -> ColumnStatistics
fromColumnStatistics raw =
  ColumnStatistics
    (getField $ Proto.numberOfValues raw)
    (getField $ Proto.intStatistics raw)
    (getField $ Proto.doubleStatistics raw)
    (getField $ Proto.stringStatistics raw)
    (getField $ Proto.bucketStatistics raw)
    (getField $ Proto.decimalStatistics raw)
    (getField $ Proto.dateStatistics raw)
    (getField $ Proto.binaryStatistics raw)
    (getField $ Proto.timestampStatistics raw)
    (getField $ Proto.hasNull raw)

data IntegerStatistics = IntegerStatistics {
    intMinimum :: Maybe Int64
  , intMaximum :: Maybe Int64
  , intSum :: Maybe Int64
  } deriving (Eq, Ord, Show, Generic)

data DoubleStatistics = DoubleStatistics {
  doubleMinimum :: Maybe Double
, doubleMaximum :: Maybe Double
, doubleSum :: Maybe Double
} deriving (Eq, Ord, Show, Generic)

data StringStatistics = StringStatistics {
    stringMinimum :: Maybe Text
  , stringMaximum :: Maybe Text
  -- sum will store the total length of all strings
  , stringSum :: Maybe Int64
  } deriving (Eq, Ord, Show, Generic)

data DecimalStatistics = DecimalStatistics {
    decimalMinimum :: Maybe Text
  , decimalMaximum :: Maybe Text
  , decimalSum :: Maybe Text
  } deriving (Eq, Ord, Show, Generic)

newtype StripeStatistics = StripeStatistics {
  colStats :: [ColumnStatistics]
} deriving (Eq, Ord, Show, Generic)

newtype BucketStatistics = BucketStatistics {
    bucketCount :: Word64
  } deriving (Eq, Ord, Show, Generic)

data DateStatistics = DateStatistics {
    -- min,max values saved as days since epoch
    dataMinimum :: Maybe Int32
  , dataMaximum :: Maybe Int32
  } deriving (Eq, Ord, Show, Generic)


data TimestampStatistics = TimestampStatistics {
    -- min,max values saved as milliseconds since epoch
    timestampMinimum :: Maybe Int64
  , timestampMaximum :: Maybe Int64
    -- min,max values saved as milliseconds since UNIX epoch
  , timestampMinimumUtc :: Maybe Int64
  , timestampMaximumUtc :: Maybe Int64
  } deriving (Eq, Ord, Show, Generic)


newtype BinaryStatistics = BinaryStatistics {
  -- sum will store the total binary blob length
  binarySum :: Maybe Int64
} deriving (Eq, Ord, Show, Generic)

data UserMetadataItem = UserMetadataItem {
    -- the user defined key
    name :: Text
    -- the user defined binary value
  , value :: ByteString
  } deriving (Eq, Ord, Show, Generic)


fromUserMetadataItem :: Proto.UserMetadataItem -> UserMetadataItem
fromUserMetadataItem raw =
  UserMetadataItem
    (getField $ Proto.name raw)
    (getField $ Proto.value raw)


toUserMetadataItem :: UserMetadataItem -> Proto.UserMetadataItem
toUserMetadataItem raw =
  Proto.UserMetadataItem
    (putField $ name raw)
    (putField $ value raw)


data RowIndexEntry = RowIndexEntry {
    positions :: [Word64]
  , rowIndexStatistics :: Maybe ColumnStatistics
  } deriving (Eq, Ord, Show)


fromRowIndexEntry :: Proto.RowIndexEntry -> RowIndexEntry
fromRowIndexEntry raw =
  RowIndexEntry
    (getField $ Proto.positions raw)
    (fmap fromColumnStatistics . getField $ Proto.rowIndexStatistics raw)


toRowIndexEntry :: RowIndexEntry -> Proto.RowIndexEntry
toRowIndexEntry raw =
  Proto.RowIndexEntry
    (putField $ positions raw)
    (mempty)


newtype RowIndex = RowIndex {
    entry :: [RowIndexEntry]
  } deriving (Eq, Ord, Show)


readRowIndex :: ByteString -> Either String RowIndex
readRowIndex bytes =
  fromRowIndex <$> Get.runGet decodeMessage bytes


putRowIndex :: Put.Putter RowIndex
putRowIndex =
  encodeMessage . toRowIndex



fromRowIndex :: Proto.RowIndex -> RowIndex
fromRowIndex raw =
  RowIndex
    (fmap fromRowIndexEntry . getField $ Proto.entry raw)


toRowIndex :: RowIndex -> Proto.RowIndex
toRowIndex raw =
  Proto.RowIndex
    (putField $ fmap toRowIndexEntry $ entry raw)


data StripeFooter = StripeFooter {
  -- the location of each stream
  streams :: [Stream]
  -- the encoding of each column
, columns :: [ColumnEncoding]
, writerTimezone :: Maybe Text
 } deriving (Eq, Ord, Show)


readStripeFooter :: ByteString -> Either String StripeFooter
readStripeFooter bytes =
  fromStripeFooter <$> Get.runGet decodeMessage bytes


putStripeFooter :: Put.Putter StripeFooter
putStripeFooter =
  encodeMessage . toProtoStripeFooter


fromStripeFooter :: Proto.StripeFooter -> StripeFooter
fromStripeFooter raw =
  StripeFooter
    (fmap fromProtoStream . getField $ Proto.streams raw)
    (fmap fromProtoColumnEncoding . getField $ Proto.columns raw)
    (getField $ Proto.writerTimezone raw)

toProtoStripeFooter :: StripeFooter -> Proto.StripeFooter
toProtoStripeFooter hydrated =
  Proto.StripeFooter
    (putField $ fmap toProtoStream $ streams hydrated)
    (putField $ fmap toProtoColumnEncoding $ columns hydrated)
    (putField $ writerTimezone hydrated)


data StreamKind
  = SK_PRESENT
  | SK_DATA
  | SK_LENGTH
  | SK_DICTIONARY_DATA
  | SK_DICTIONARY_COUNT
  | SK_SECONDARY
  | SK_ROW_INDEX
  | SK_BLOOM_FILTER
  deriving (Eq, Ord, Show, Enum)

fromProtoStreamKind :: Proto.StreamKind -> StreamKind
fromProtoStreamKind = \case
  Proto.SK_PRESENT -> SK_PRESENT
  Proto.SK_DATA -> SK_DATA
  Proto.SK_LENGTH -> SK_LENGTH
  Proto.SK_DICTIONARY_DATA -> SK_DICTIONARY_DATA
  Proto.SK_DICTIONARY_COUNT -> SK_DICTIONARY_COUNT
  Proto.SK_SECONDARY -> SK_SECONDARY
  Proto.SK_ROW_INDEX -> SK_ROW_INDEX
  Proto.SK_BLOOM_FILTER -> SK_BLOOM_FILTER

toProtoStreamKind :: StreamKind -> Proto.StreamKind
toProtoStreamKind = \case
  SK_PRESENT -> Proto.SK_PRESENT
  SK_DATA -> Proto.SK_DATA
  SK_LENGTH -> Proto.SK_LENGTH
  SK_DICTIONARY_DATA -> Proto.SK_DICTIONARY_DATA
  SK_DICTIONARY_COUNT -> Proto.SK_DICTIONARY_COUNT
  SK_SECONDARY -> Proto.SK_SECONDARY
  SK_ROW_INDEX -> Proto.SK_ROW_INDEX
  SK_BLOOM_FILTER -> Proto.SK_BLOOM_FILTER

data Stream = Stream {
  -- if you add new index stream kinds, you need to make sure to update
  -- StreamName to ensure it is added to the stripe in the right area
  streamKind   :: Maybe StreamKind
, streamColumn :: Maybe Word32
, streamLength :: Maybe Word64
} deriving (Eq, Ord, Show)


fromProtoStream :: Proto.Stream -> Stream
fromProtoStream raw =
  Stream
    (fmap fromProtoStreamKind $ getField $ Proto.streamKind raw)
    (getField $ Proto.streamColumn raw)
    (getField $ Proto.streamLength raw)


toProtoStream :: Stream -> Proto.Stream
toProtoStream raw =
  Proto.Stream
    (putField $ fmap toProtoStreamKind $ streamKind raw)
    (putField $ streamColumn raw)
    (putField $ streamLength raw)


data ColumnEncodingKind
  = DIRECT
  | DICTIONARY
  | DIRECT_V2
  | DICTIONARY_V2
  deriving (Eq, Ord, Show, Enum)

fromProtoColumnEncodingKind :: Proto.ColumnEncodingKind -> ColumnEncodingKind
fromProtoColumnEncodingKind = \case
  Proto.DIRECT -> DIRECT
  Proto.DICTIONARY -> DICTIONARY
  Proto.DIRECT_V2 -> DIRECT_V2
  Proto.DICTIONARY_V2 -> DICTIONARY_V2

toProtoColumnEncodingKind :: ColumnEncodingKind -> Proto.ColumnEncodingKind
toProtoColumnEncodingKind = \case
  DIRECT -> Proto.DIRECT
  DICTIONARY -> Proto.DICTIONARY
  DIRECT_V2 -> Proto.DIRECT_V2
  DICTIONARY_V2 -> Proto.DICTIONARY_V2

data ColumnEncoding = ColumnEncoding {
  columnEncodingKind :: ColumnEncodingKind
, columnEncodingdictionarySize :: Maybe Word32
} deriving (Eq, Ord, Show)


fromProtoColumnEncoding :: Proto.ColumnEncoding -> ColumnEncoding
fromProtoColumnEncoding raw =
  ColumnEncoding
    (fromProtoColumnEncodingKind $ getField $ Proto.columnEncodingKind raw)
    (getField $ Proto.columnEncodingdictionarySize raw)

toProtoColumnEncoding :: ColumnEncoding -> Proto.ColumnEncoding
toProtoColumnEncoding raw =
  Proto.ColumnEncoding
    (putField $ toProtoColumnEncodingKind $ columnEncodingKind raw)
    (putField $ columnEncodingdictionarySize raw)
