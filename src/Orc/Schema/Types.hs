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
  , Footer (..)
  , StripeFooter (..)

  , RowIndexEntry (..)
  , RowIndex (..)

  , UserMetadataItem (..)
  , ColumnStatistics (..)
  ) where

import           Data.Int
import           Data.ByteString (ByteString)
import           Data.Text (Text)
import           Data.Word
import           GHC.Generics (Generic)

import           Orc.Data.Data (StructField (..))

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


data ColumnStatistics = ColumnStatistics {
 -- the number of values
   numberOfValues :: Maybe Word64
 -- At most one of these has a value for any column
--  , intStatistics :: Maybe IntegerStatistics
--  , doubleStatistics :: Maybe DoubleStatistics
--  , stringStatistics :: Maybe StringStatistics
--  , bucketStatistics :: Maybe BucketStatistics
--  , decimalStatistics :: Maybe DecimalStatistics
--  , dateStatistics :: Maybe DateStatistics
--  , binaryStatistics :: Maybe BinaryStatistics
--  , timestampStatistics :: Maybe TimestampStatistics
 , hasNull :: Maybe Bool
} deriving (Eq, Ord, Show, Generic)


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


data RowIndexEntry = RowIndexEntry {
    positions :: [Word64]
  , rowIndexStatistics :: Maybe ColumnStatistics
  } deriving (Eq, Ord, Show)


newtype RowIndex = RowIndex {
    entry :: [RowIndexEntry]
  } deriving (Eq, Ord, Show)


data StripeFooter = StripeFooter {
  -- the location of each stream
  streams :: [Stream]
  -- the encoding of each column
, columns :: [ColumnEncoding]
, writerTimezone :: Maybe Text
 } deriving (Eq, Ord, Show)


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


data Stream = Stream {
  -- if you add new index stream kinds, you need to make sure to update
  -- StreamName to ensure it is added to the stripe in the right area
  streamKind   :: Maybe StreamKind
, streamColumn :: Maybe Word32
, streamLength :: Maybe Word64
} deriving (Eq, Ord, Show)


data ColumnEncodingKind
  = DIRECT
  | DICTIONARY
  | DIRECT_V2
  | DICTIONARY_V2
  deriving (Eq, Ord, Show, Enum)



data ColumnEncoding = ColumnEncoding {
  columnEncodingKind :: ColumnEncodingKind
, columnEncodingdictionarySize :: Maybe Word32
} deriving (Eq, Ord, Show)

