module Orc.Data.Data (
    StructField (..)
  , StructFieldName (..)
  ) where

import         P

data StructField a = StructField {
    fieldName :: StructFieldName
  , fieldValue :: a
  } deriving (Eq, Ord, Show)

newtype StructFieldName = StructFieldName {
    getFieldName :: Text
  } deriving (Eq, Ord, Show)
