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
  = Struct !(Cons Boxed.Vector (StructField Column))
  | List !(Storable.Vector Int64) !Column
  | Map !Column !Column

  | Nullable (Storable.Vector Word8) Column

  | Integer !(Storable.Vector Int64)
  deriving (Eq, Ord, Show)
