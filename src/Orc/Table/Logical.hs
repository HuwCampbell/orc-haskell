module Orc.Table.Logical (
  Row (..)
) where

import           P

import           Data.ByteString (ByteString)
import           Data.Word (Word8)
import           Data.WideWord (Int128)

import           Numeric.Decimal (Decimal128)

import           Orc.Data.Data
import qualified X.Data.Vector as Boxed

data Row
  -- Composite Columns
  = Struct    !(Boxed.Vector (StructField Row))
  | Union     !Word8 !Row
  | List      !(Boxed.Vector Row)
  | Map       !(Boxed.Vector (Row,Row))

  -- Primitive Columns
  | Bool      !Bool
  | Bytes     !Word8

  | Short     !Int16
  | Integer   !Int32
  | Long      !Int64

  | Decimal   !Decimal128
  | Date      !Int64
  | Timestamp !Int64

  | Float     !Float
  | Double    !Double

  | String    !ByteString
  | Char      !ByteString
  | VarChar   !ByteString

  | Binary    !ByteString

  -- For Nullable columns.
  | Partial   !(Maybe Row)
  deriving (Eq, Show)
