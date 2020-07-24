{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Orc.Table.Logical (
    Row (..)

  -- * Projection functions
  , takeString
  , takeChar
  , takeVarChar
  , takeBinary
  , takeBool
  , takeByte
  , takeShort
  , takeInteger
  , takeLong
  , takeDate
  , takeFloat
  , takeDouble
  , takeAnonymousStruct
  , takeStruct
  , takeUnion
  , takeList
  , takeMap
  , takePartials
  , takeDecimal
  , takeTimestamp
) where

import           Orc.Prelude

import           Data.ByteString (ByteString)
import           Data.Scientific (Scientific)
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
  | Byte      !Word8

  | Short     !Int16
  | Integer   !Int32
  | Long      !Int64

  | Decimal   !Scientific
  | Date      !Orc.Day
  | Timestamp !Orc.Timestamp

  | Float     !Float
  | Double    !Double

  | String    !ByteString
  | Char      !ByteString
  | VarChar   !ByteString

  | Binary    !ByteString

  -- For partial columns.
  | Null
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

takeByte :: Row -> Maybe Word8
takeByte (Byte x) = Just x
takeByte _        = Nothing

takeShort :: Row -> Maybe Int16
takeShort (Short x) = Just x
takeShort _         = Nothing

takeInteger :: Row -> Maybe Int32
takeInteger (Integer x) = Just x
takeInteger _           = Nothing

takeLong :: Row -> Maybe Int64
takeLong (Long x) = Just x
takeLong _        = Nothing

takeDecimal :: Row -> Maybe Scientific
takeDecimal (Decimal x) = Just x
takeDecimal _           = Nothing

takeTimestamp :: Row -> Maybe Orc.Timestamp
takeTimestamp (Timestamp x) = Just x
takeTimestamp _             = Nothing

takeDate :: Row -> Maybe Int64
takeDate (Date (Orc.Day x)) = Just x
takeDate _                  = Nothing

takeFloat :: Row -> Maybe Float
takeFloat (Float x) = Just x
takeFloat _         = Nothing

takeDouble :: Row -> Maybe Double
takeDouble (Double x) = Just x
takeDouble _          = Nothing

takeAnonymousStruct :: Row -> Maybe (Boxed.Vector Row)
takeAnonymousStruct (Struct x) = Just (fmap fieldValue x)
takeAnonymousStruct _          = Nothing

takeStruct :: Row -> Maybe (Boxed.Vector (StructField Row))
takeStruct (Struct x) = Just x
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

takePartials :: Row -> Maybe Row
takePartials Null = Nothing
takePartials x    = Just x
