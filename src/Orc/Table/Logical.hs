{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Orc.Table.Logical (
    Row (..)

  , takeString
  , takeChar
  , takeVarChar
  , takeBinary
  , takeBool
  , takeBytes
  , takeShort
  , takeInteger
  , takeLong
  , takeDate
  , takeFloat
  , takeDouble
  , takeStruct
  , takeUnion
  , takeList
  , takeMap
  , takePartials
  , takeDecimal
) where

import           Orc.Prelude

import           Data.ByteString (ByteString)
import           Data.Decimal (Decimal)
import           Data.Word (Word8)

import qualified Data.Vector as Boxed

import           Orc.Data.Data
import qualified Orc.Data.Time as Orc

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

  | Decimal   !Decimal
  | Date      !Orc.Day
  | Timestamp !Orc.Timestamp

  | Float     !Float
  | Double    !Double

  | String    !ByteString
  | Char      !ByteString
  | VarChar   !ByteString

  | Binary    !ByteString

  -- For nullable columns.
  | Partial   !(Maybe' Row)
  deriving (Eq, Show)






takeString :: Row -> Maybe ByteString
takeString (String x) = Just x
takeString _          = Nothing

takeChar :: Row -> Maybe ByteString
takeChar (Char x) = Just x
takeChar _        = Nothing

takeVarChar :: Row -> Maybe ByteString
takeVarChar (VarChar x) = Just x
takeVarChar _           = Nothing

takeBinary :: Row -> Maybe ByteString
takeBinary (Binary x) = Just x
takeBinary _          = Nothing

takeBool :: Row -> Maybe Bool
takeBool (Bool x) = Just x
takeBool _        = Nothing

takeBytes :: Row -> Maybe Word8
takeBytes (Bytes x) = Just x
takeBytes _         = Nothing

takeShort :: Row -> Maybe Int16
takeShort (Short x) = Just x
takeShort _         = Nothing

takeInteger :: Row -> Maybe Int32
takeInteger (Integer x) = Just x
takeInteger _           = Nothing

takeLong :: Row -> Maybe Int64
takeLong (Long x) = Just x
takeLong _        = Nothing

takeDecimal :: Row -> Maybe Decimal
takeDecimal (Decimal x) = Just x
takeDecimal _           = Nothing

takeDate :: Row -> Maybe Int64
takeDate (Date (Orc.Day x)) = Just x
takeDate _                  = Nothing

takeFloat :: Row -> Maybe Float
takeFloat (Float x) = Just x
takeFloat _         = Nothing

takeDouble :: Row -> Maybe Double
takeDouble (Double x) = Just x
takeDouble _          = Nothing

takeStruct :: Row -> Maybe (Boxed.Vector Row)
takeStruct (Struct x) = Just (fmap fieldValue x)
takeStruct _          = Nothing

takeUnion :: Row -> Maybe (Word8, Row)
takeUnion (Union w r) = Just (w, r)
takeUnion _           = Nothing

takeList :: Row -> Maybe (Boxed.Vector Row)
takeList (List x) = Just x
takeList _        = Nothing

takeMap :: Row -> Maybe (Boxed.Vector (Row,Row))
takeMap (Map x) = Just x
takeMap _       = Nothing

takePartials :: Row -> Maybe (Maybe' Row)
takePartials (Partial x) = Just x
takePartials _           = Nothing

