{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Orc.Table.Logical (
    Row (..)
  , ppRow
) where

import           P

import           Data.ByteString (ByteString)
import           Data.Decimal (Decimal)
import           Data.Word (Word8)
import           Data.WideWord (Int128)
import qualified Data.List as List
import qualified Data.ByteString.Lazy as Lazy
import qualified Data.ByteString.Builder as Builder
import qualified Data.Vector as Vector
import qualified Data.Text as Text


import           Orc.Data.Data
import qualified Data.Vector as Boxed

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
  | Date      !Int64
  | Timestamp !Int64

  | Float     !Float
  | Double    !Double

  | String    !ByteString
  | Char      !ByteString
  | VarChar   !ByteString

  | Binary    !ByteString

  -- For nullable columns.
  | Partial   !(Maybe' Row)
  deriving (Eq, Show)


ppRow :: Row -> Lazy.ByteString
ppRow =
  Builder.toLazyByteString . (<> Builder.char8 '\n') . buildRow


buildRow :: Row -> Builder.Builder
buildRow = \case
  Struct fs ->
    let
      fields = mconcat . List.intersperse (Builder.byteString ", ") . fmap buildField . Vector.toList
    in
      Builder.char8 '{' <> fields fs <> Builder.char8 '}'

  Union i r ->
    Builder.word8Dec i <> (Builder.byteString ": ") <> buildRow r

  List rs ->
    let
      vals = mconcat . List.intersperse (Builder.byteString ", ") . fmap buildRow . Vector.toList
    in
      Builder.char8 '[' <> vals rs <> Builder.char8 ']'

  Map rs ->
    let
      vals = mconcat . List.intersperse (Builder.byteString ", ") . fmap buildMap . Vector.toList
    in
      Builder.char8 '{' <> vals rs <> Builder.char8 '}'

  Bool b ->
    Builder.char8 $
      if b then 'T' else 'F'

  Bytes b ->
    Builder.word8Dec b

  Short b ->
    Builder.int16Dec b

  Integer b ->
    Builder.int32Dec b

  Long b ->
    Builder.int64Dec b

  Decimal b ->
    Builder.stringUtf8 $
      show b

  Date b ->
    Builder.int64Dec b

  Timestamp b ->
    Builder.int64Dec b

  Float b ->
    Builder.floatDec b

  Double b ->
    Builder.doubleDec b

  String b ->
    Builder.byteString b

  Char b ->
    Builder.byteString b

  VarChar b ->
    Builder.byteString b

  Binary b ->
    Builder.byteString b

  Partial mv ->
    maybe' (Builder.byteString "null") buildRow mv

buildField :: StructField Row -> Builder.Builder
buildField (StructField (StructFieldName n) r) =
  Builder.stringUtf8 (Text.unpack n) <> Builder.byteString ": " <> buildRow r

buildMap :: (Row,Row) -> Builder.Builder
buildMap (k, v) =
  buildRow k <> Builder.byteString ": " <> buildRow v
