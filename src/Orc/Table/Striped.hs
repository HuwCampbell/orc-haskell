module Orc.Table.Striped (
  Column (..)
) where

import           Orc.Prelude

import           Data.ByteString (ByteString)
import           Data.Word (Word8, Word32)
import           Data.WideWord (Int128)

import           Orc.Data.Data

import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

data Column
  -- Composite Columns
  = Struct    !(Boxed.Vector (StructField Column))
  | Union     !(Storable.Vector Word8) !(Boxed.Vector Column)
  | List      !(Storable.Vector Word32) !Column
  | Map       !(Storable.Vector Word32) !Column !Column

  -- Danger:
  --   Bool and Presence vectors can be up to 7 bits longer
  --   than one might think, as they are represented as packed
  --   bytes.

  -- Primitive Columns
  | Bool      !(Storable.Vector Bool)
  | Bytes     !(Storable.Vector Word8)

  | Short     !(Storable.Vector Int16)
  | Integer   !(Storable.Vector Int32)
  | Long      !(Storable.Vector Int64)

  | Decimal   !(Storable.Vector Int128) !(Storable.Vector Int16)
  | Date      !(Storable.Vector Int64)
  | Timestamp !(Storable.Vector Int64)  !(Storable.Vector Word64)

  | Float     !(Storable.Vector Float)
  | Double    !(Storable.Vector Double)

  | String    !(Boxed.Vector ByteString)
  | Char      !(Boxed.Vector ByteString)
  | VarChar   !(Boxed.Vector ByteString)

  | Binary    !(Boxed.Vector ByteString)

  -- For Nullable columns.
  | Partial   !(Storable.Vector Bool) !Column
  deriving (Eq, Show)
