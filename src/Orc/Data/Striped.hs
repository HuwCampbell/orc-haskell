module Orc.Data.Striped (
  Column (..)
) where

import           P

import           Data.ByteString (ByteString)
import           Data.Ratio (Ratio)
import           Data.Word (Word8, Word64)
import           Data.WideWord (Int128)

import           Orc.Data.Data
import           Orc.Data.Segmented
import           Orc.Schema.Types (Type, ColumnEncodingKind)

import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable

data Column
  -- Composite Columns
  = Struct  ![StructField Column]
  | Union   !(Storable.Vector Word8) ![Column]
  | List    !(Storable.Vector Int64) !Column
  | Map     !Column !Column

  -- Primitive Columns
  | Bool    !(Storable.Vector Word8)
  | Bytes   !(Storable.Vector Word8)

  | Short   !(Storable.Vector Int16)
  | Integer !(Storable.Vector Int32)
  | Long    !(Storable.Vector Int64)

  | Decimal !(Storable.Vector Int128) !(Storable.Vector Int16)

  | Float   !(Storable.Vector Float)
  | Double  !(Storable.Vector Double)

  | String  !(Boxed.Vector ByteString)
  | Char    !(Boxed.Vector ByteString)
  | VarChar !(Boxed.Vector ByteString)

  -- For Nullable columns.
  | Partial (Storable.Vector Word8) Column
  | UnhandleColumn Type (Maybe ColumnEncodingKind)
  deriving (Eq, Show)
