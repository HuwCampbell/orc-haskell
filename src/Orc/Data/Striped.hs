module Orc.Data.Striped (
  Column (..)
) where

import           P

import           Data.Word (Word8)

import           Orc.Data.Data

import           X.Data.Vector.Cons (Cons)
import qualified X.Data.Vector as Boxed
import qualified X.Data.Vector.Storable as Storable

data Column
  = Struct ![StructField Column]
  | List !(Storable.Vector Int64) !Column
  | Map !Column !Column

  | Partial (Storable.Vector Word8) Column

  | Bool !(Storable.Vector Word8)
  | Bytes !(Storable.Vector Word8)
  | Integer !(Storable.Vector Int64)
  deriving (Eq, Ord, Show)
