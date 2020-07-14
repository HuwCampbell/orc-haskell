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
import qualified Data.Text.Lazy as Lazy
import qualified Data.Text.Internal.Builder as Builder
import qualified Data.Vector as Vector


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


ppRow :: Row -> Text
ppRow =
  Lazy.toStrict . Builder.toLazyText . buildRow


buildRow :: Row -> Builder.Builder
buildRow = \case
  Struct fs ->
    let
      fields = List.intersperse (Builder.fromText ", ") . fmap buildField . Vector.toList
    in
      Builder.singleton '{' <> mconcat (fields fs) <> Builder.singleton '}'

  Union i r ->
    Builder.fromString (show i) <> Builder.fromText ": " <> buildRow r

  List rs ->
    let
      vals = List.intersperse (Builder.fromText ", ") . fmap buildRow . Vector.toList
    in
      Builder.singleton '[' <> mconcat (vals rs) <> Builder.singleton ']'

  Map rs ->
    let
      vals = List.intersperse (Builder.fromText ", ") . fmap buildMap . Vector.toList
    in
      Builder.singleton '{' <> mconcat (vals rs) <> Builder.singleton '}'

  Bool b ->
    Builder.fromText $
      if b then "T" else "F"

  Bytes b ->
    Builder.fromString $
      show b

  Short b ->
    Builder.fromString $
      show b

  Integer b ->
    Builder.fromString $
      show b

  Long b ->
    Builder.fromString $
      show b

  Decimal b ->
    Builder.fromString $
      show b

  Date b ->
    Builder.fromString $
      show b

  Timestamp b ->
    Builder.fromString $
      show b

  Float b ->
    Builder.fromString $
      show b

  Double b ->
    Builder.fromString $
      show b

  String b ->
    Builder.fromString $
      show b

  Char b ->
    Builder.fromString $
      show b

  VarChar b ->
    Builder.fromString $
      show b

  Binary b ->
    Builder.fromString $
      show b

  Partial mv ->
    maybe' (Builder.fromString "null") buildRow mv

buildField :: StructField Row -> Builder.Builder
buildField (StructField (StructFieldName n) r) =
  Builder.fromText n <> Builder.fromText ": " <> buildRow r

buildMap :: (Row,Row) -> Builder.Builder
buildMap (k, v) =
  buildRow k <> Builder.fromText ": " <> buildRow v
