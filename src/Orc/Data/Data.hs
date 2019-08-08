{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE FlexibleContexts    #-}

module Orc.Data.Data (
    StructField (..)
  , StructFieldName (..)
  , Indexed (..)

  , makeIndexed
  , nextIndex
  , currentIndex
  , currentValue
  ) where

import         Control.Monad.Except

import         Data.Word (Word32)
import         Data.String (String)
import         P

data StructField a = StructField {
    fieldName :: StructFieldName
  , fieldValue :: a
  } deriving (Eq, Ord, Show, Functor, Foldable, Traversable)

newtype StructFieldName = StructFieldName {
    getFieldName :: Text
  } deriving (Eq, Ord, Show)

data Indexed a =
  Indexed {
    _currentIndex :: Word32
  , _indexed :: [a]
  } deriving (Eq, Ord, Show, Functor, Foldable, Traversable)

makeIndexed :: [a] -> Indexed a
makeIndexed =
  Indexed 0

nextIndex :: Indexed a -> Indexed a
nextIndex (Indexed i as) =
  Indexed (i +1) (drop 1 as)

currentIndex :: Indexed a -> Word32
currentIndex (Indexed i _) = i

currentValue :: Indexed a -> Maybe a
currentValue (Indexed _ as) = head as
