{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds     #-}

module Orc.Schema.Types (
    Proto.CompressionKind (..)
  , Proto.StreamKind (..)
  , Proto.ColumnEncodingKind (..)

  , PostScript (..)
  , readPostScript
  , Footer (..)
  , readFooter
  , Type (..)

  , StripeFooter (..)
  , readStripeFooter
  , StripeInformation (..)
  , ColumnEncoding (..)

  , RowIndexEntry (..)
  , RowIndex (..)
  , readRowIndex

  , Stream (..)
  , readStream
  ) where

import           Data.Int
import           Data.ByteString (ByteString)
import           Data.Word
import           Data.ProtocolBuffers
import           Data.Text (Text)
import           GHC.Generics (Generic)
import           Data.Traversable (for)

import           Control.Monad.Trans.State (state, runState)

import qualified Data.Serialize.Get as Get

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

data PostScript = PostScript {
    -- the length of the footer section in bytes
    -- NOTE: Changed to Required from Optional
    footerLength :: Word64
    -- the kind of generic compression used
  , compression ::  Maybe Proto.CompressionKind
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
readPostScript bytes = do
  raw <- Get.runGet decodeMessage bytes
  return $
    PostScript
      (getField $ Proto.footerLength raw)
      (getField $ Proto.compression raw)
      (getField $ Proto.compressionBlockSize raw)
      (getField $ Proto.version raw)
      (getField $ Proto.metadataLength raw)
      (getField $ Proto.magic raw)

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

fromProtoFooter :: Proto.Footer -> Either String Footer
fromProtoFooter raw =
  Footer
    (getField $ Proto.headerLength raw)
    (getField $ Proto.contentLength raw)
    (fmap fromStripeInformation . getField $ Proto.stripes raw)
    <$> (fromTypes . getField $ Proto.types raw)
    <*> pure (fmap fromUserMetadataItem . getField $ Proto.metadata raw)
    <*> pure (getField $ Proto.numberOfRows raw)
    <*> pure (fmap fromColumnStatistics $ getField $ Proto.statistics raw)
    <*> pure (getField $ Proto.rowIndexStride raw)


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

-- data Type = Type {
--   -- the kind of this type
--   kind ::Proto.Kind
--   -- the type ids of any subcolumns for list, map, struct, or union
-- , subtypes :: [Word32]
--   -- the list of field names for struct
-- , fieldNames :: [Text]
--   -- the maximum length of the type for varchar or char in UTF-8 characters
-- , maximumLength :: Maybe Word32
--   -- the precision and scale for decimal
-- , precision :: Maybe Word32
-- , scale :: Maybe Word32
-- } deriving (Eq, Ord, Show, Generic)

fromTypes :: [Proto.Type] -> Either String Type
fromTypes typs
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
      Left $ "Leftovers! Coundn't parse " <> show typs

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


data RowIndexEntry = RowIndexEntry {
    positions :: [Word64]
  , rowIndexStatistics :: Maybe ColumnStatistics
  } deriving (Eq, Ord, Show)


fromRowIndexEntry :: Proto.RowIndexEntry -> RowIndexEntry
fromRowIndexEntry raw =
  RowIndexEntry
    (getField $ Proto.positions raw)
    (fmap fromColumnStatistics . getField $ Proto.rowIndexStatistics raw)


newtype RowIndex = RowIndex {
    entry :: [RowIndexEntry]
  } deriving (Eq, Ord, Show)


readRowIndex :: ByteString -> Either String RowIndex
readRowIndex bytes =
  fromRowIndex <$> Get.runGet decodeMessage bytes


fromRowIndex :: Proto.RowIndex -> RowIndex
fromRowIndex raw =
  RowIndex
    (fmap fromRowIndexEntry . getField $ Proto.entry raw)


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


fromStripeFooter :: Proto.StripeFooter -> StripeFooter
fromStripeFooter raw =
  StripeFooter
    (fmap fromStream . getField $ Proto.streams raw)
    (fmap fromColumnEncoding . getField $ Proto.columns raw)
    (getField $ Proto.writerTimezone raw)


data Stream = Stream {
  -- if you add new index stream kinds, you need to make sure to update
  -- StreamName to ensure it is added to the stripe in the right area
  streamKind   :: Maybe Proto.StreamKind
, streamColumn :: Maybe Word32
, streamLength :: Maybe Word64
} deriving (Eq, Ord, Show)


readStream :: ByteString -> Either String Stream
readStream bytes =
  fromStream <$> Get.runGet decodeMessage bytes


fromStream :: Proto.Stream -> Stream
fromStream raw =
  Stream
    (getField $ Proto.streamKind raw)
    (getField $ Proto.streamColumn raw)
    (getField $ Proto.streamLength raw)


data ColumnEncoding = ColumnEncoding {
  columnEncodingKind :: Proto.ColumnEncodingKind
, columnEncodingdictionarySize :: Maybe Word32
} deriving (Eq, Ord, Show)


fromColumnEncoding :: Proto.ColumnEncoding -> ColumnEncoding
fromColumnEncoding raw =
  ColumnEncoding
    (getField $ Proto.columnEncodingKind raw)
    (getField $ Proto.columnEncodingdictionarySize raw)
