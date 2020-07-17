{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DataKinds     #-}

module Orc.Serial.Protobuf.Schema.Definitions where

import Data.Int
import Data.ByteString
import Data.Word
import Data.ProtocolBuffers
import Data.Text
import GHC.Generics (Generic)

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
    footerLength :: Required 1 (Value Word64)
    -- the kind of generic compression used
  , compression ::  Optional 2 (Enumeration CompressionKind)
    -- the maximum size of each compression chunk
  , compressionBlockSize :: Optional 3 (Value Word64)
    -- the version of the writer
  , version :: Packed 4 (Value Word32)
    -- the length of the metadata section in bytes
  , metadataLength :: Optional 5 (Value Word64)
    -- the fixed string "ORC"
  , magic :: Optional 8000 (Value Text)
  } deriving (Eq, Ord, Show, Generic)

instance Encode PostScript
instance Decode PostScript

data Footer = Footer {
    -- the length of the file header in bytes (always 3)
    headerLength :: Optional 1 (Value Word64)
    -- the length of the file header and body in bytes
  , contentLength :: Optional 2 (Value Word64)
    -- the information about the stripes
  , stripes :: Repeated 3 (Message StripeInformation)
    -- the schema information
  , types :: Repeated 4 (Message Type)
    -- the user metadata that was added
  , metadata :: Repeated 5 (Message UserMetadataItem)
    -- the total number of rows in the file
  , numberOfRows :: Optional 6 (Value Word64)
    -- the statistics of each column across the file
  , statistics :: Repeated 7 (Message ColumnStatistics)
      -- the maximum number of rows in each index entry
  , rowIndexStride :: Optional 8 (Value Word32)
  } deriving (Eq, Ord, Show, Generic)

instance Encode Footer
instance Decode Footer

data StripeInformation = StripeInformation {
    -- the start of the stripe within the file
    offset :: Optional 1 (Value Word64)
    -- the length of the indexes in bytes
  , indexLength :: Optional 2 (Value Word64)
    -- the length of the data in bytes
  , dataLength :: Optional 3 (Value Word64)
    -- the length of the footer in bytes
  , siFooterLength :: Optional 4 (Value Word64)
    -- the number of rows in the stripe
  , siNumberOfRows :: Optional 5 (Value Word64)
  } deriving (Eq, Ord, Show, Generic)

instance Encode StripeInformation
instance Decode StripeInformation

data Kind
  = BOOLEAN
  | BYTE
  | SHORT
  | INT
  | LONG
  | FLOAT
  | DOUBLE
  | STRING
  | BINARY
  | TIMESTAMP
  | LIST
  | MAP
  | STRUCT
  | UNION
  | DECIMAL
  | DATE
  | VARCHAR
  | CHAR
  deriving (Eq, Ord, Show, Enum)

data Type = Type {
    -- the kind of this type
    kind :: Required 1 (Enumeration Kind)
    -- the type ids of any subcolumns for list, map, struct, or union
  , subtypes :: Packed 2 (Value Word32)
    -- the list of field names for struct
  , fieldNames :: Repeated 3 (Value Text)
    -- the maximum length of the type for varchar or char in UTF-8 characters
  , maximumLength :: Optional 4 (Value Word32)
    -- the precision and scale for decimal
  , precision :: Optional 5 (Value Word32)
  , scale :: Optional 6 (Value Word32)
  } deriving (Eq, Ord, Show, Generic)

instance Encode Type
instance Decode Type

data ColumnStatistics = ColumnStatistics {
 -- the number of values
   numberOfValues :: Optional 1 (Value Word64)
 -- At most one of these has a value for any column
 , intStatistics :: Optional 2 (Message IntegerStatistics)
 , doubleStatistics :: Optional 3 (Message DoubleStatistics)
 , stringStatistics :: Optional 4 (Message StringStatistics)
 , bucketStatistics :: Optional 5 (Message BucketStatistics)
 , decimalStatistics :: Optional 6 (Message DecimalStatistics)
 , dateStatistics :: Optional 7 (Message DateStatistics)
 , binaryStatistics :: Optional 8 (Message BinaryStatistics)
 , timestampStatistics :: Optional 9 (Message TimestampStatistics)
 , hasNull :: Optional 10 (Value Bool)
} deriving (Eq, Ord, Show, Generic)

instance Encode ColumnStatistics
instance Decode ColumnStatistics

data IntegerStatistics = IntegerStatistics {
    intMinimum :: Optional 1 (Value (Signed Int64))
  , intMaximum :: Optional 2 (Value (Signed Int64))
  , intSum :: Optional 3 (Value (Signed Int64))
  } deriving (Eq, Ord, Show, Generic)

instance Encode IntegerStatistics
instance Decode IntegerStatistics

data DoubleStatistics = DoubleStatistics {
    doubleMinimum :: Optional 1 (Value Double)
  , doubleMaximum :: Optional 2 (Value Double)
  , doubleSum :: Optional 3 (Value Double)
  } deriving (Eq, Ord, Show, Generic)

instance Encode DoubleStatistics
instance Decode DoubleStatistics

data StringStatistics = StringStatistics {
    stringMinimum :: Optional 1 (Value Text)
  , stringMaximum :: Optional 2 (Value Text)
  -- sum will store the total length of all strings
  , stringSum :: Optional 3 (Value (Signed Int64))
  } deriving (Eq, Ord, Show, Generic)

instance Encode StringStatistics
instance Decode StringStatistics

newtype BucketStatistics = BucketStatistics {
    bucketCount :: Packed 1 (Value Word64)
  } deriving (Eq, Ord, Show, Generic)

instance Encode BucketStatistics
instance Decode BucketStatistics

data DecimalStatistics = DecimalStatistics {
    decimalMinimum :: Optional 1 (Value Text)
  , decimalMaximum :: Optional 2 (Value Text)
  , decimalSum :: Optional 3 (Value Text)
  } deriving (Eq, Ord, Show, Generic)

instance Encode DecimalStatistics
instance Decode DecimalStatistics

data DateStatistics = DateStatistics {
    -- min,max values saved as days since epoch
    dataMinimum :: Optional 1 (Value (Signed Int32))
  , dataMaximum :: Optional 2 (Value (Signed Int32))
  } deriving (Eq, Ord, Show, Generic)

instance Encode DateStatistics
instance Decode DateStatistics

data TimestampStatistics = TimestampStatistics {
    -- min,max values saved as milliseconds since epoch
    timestampMinimum :: Optional 1 (Value (Signed Int64))
  , timestampMaximum :: Optional 2 (Value (Signed Int64))
    -- min,max values saved as milliseconds since UNIX epoch
  , timestampMinimumUtc :: Optional 3 (Value (Signed Int64))
  , timestampMaximumUtc :: Optional 4 (Value (Signed Int64))
  } deriving (Eq, Ord, Show, Generic)

instance Encode TimestampStatistics
instance Decode TimestampStatistics

newtype BinaryStatistics = BinaryStatistics {
    -- sum will store the total binary blob length
    binarySum :: Optional 1 (Value (Signed Int64))
  } deriving (Eq, Ord, Show, Generic)

instance Encode BinaryStatistics
instance Decode BinaryStatistics

data UserMetadataItem = UserMetadataItem {
    -- the user defined key
    name :: Required 1 (Value Text)
    -- the user defined binary value
  , value :: Required 2 (Value ByteString)
  } deriving (Eq, Ord, Show, Generic)

instance Encode UserMetadataItem
instance Decode UserMetadataItem

newtype StripeStatistics = StripeStatistics {
    colStats :: Repeated 1 (Message ColumnStatistics)
  } deriving (Eq, Ord, Show, Generic)

instance Encode StripeStatistics
instance Decode StripeStatistics

newtype Metadata = Metadata {
    stripeStats :: Repeated 1 (Message StripeStatistics)
  } deriving (Eq, Ord, Show, Generic)

instance Encode Metadata
instance Decode Metadata

data RowIndexEntry = RowIndexEntry {
    positions :: Packed 1 (Value Word64)
  , rowIndexStatistics :: Optional 2 (Message ColumnStatistics)
  } deriving (Eq, Ord, Show, Generic)

instance Encode RowIndexEntry
instance Decode RowIndexEntry

newtype RowIndex = RowIndex {
    entry :: Repeated 1 (Message RowIndexEntry)
  } deriving (Eq, Ord, Show, Generic)

instance Encode RowIndex
instance Decode RowIndex

data StripeFooter = StripeFooter {
    -- the location of each stream
    streams :: Repeated 1 (Message Stream)
    -- the encoding of each column
  , columns :: Repeated 2 (Message ColumnEncoding)
  , writerTimezone :: Optional 3 (Value Text)
   } deriving (Eq, Ord, Show, Generic)

instance Encode StripeFooter
instance Decode StripeFooter

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
    streamKind :: Optional 1 (Enumeration StreamKind)
  , streamColumn :: Optional 2 (Value Word32)
  , streamLength :: Optional 3 (Value Word64)
  } deriving (Eq, Ord, Show, Generic)

instance Encode Stream
instance Decode Stream

data ColumnEncodingKind
  = DIRECT
  | DICTIONARY
  | DIRECT_V2
  | DICTIONARY_V2
  deriving (Eq, Ord, Show, Enum)


data ColumnEncoding = ColumnEncoding {
    columnEncodingKind :: Required 1 (Enumeration ColumnEncodingKind)
  , columnEncodingdictionarySize :: Optional 2 (Value Word32)
  } deriving (Eq, Ord, Show, Generic)

instance Encode ColumnEncoding
instance Decode ColumnEncoding
