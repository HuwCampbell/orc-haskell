{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Orc.Table.Logical (
    Row (..)
  , ppRow
) where

import           P

-- import           Chronos (Time, Day, builderUtf8_Ymd, dayToDate)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.Char (ord)
import           Data.Decimal (Decimal)
import           Data.Word (Word8)
import qualified Data.List as List
import qualified Data.ByteString.Lazy as Lazy
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Builder.Prim as Prim
import           Data.ByteString.Builder.Prim ((>*<), (>$<))
import qualified Data.Vector as Boxed
import           Data.Text.Encoding (encodeUtf8BuilderEscaped)

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
      fields = mconcat . List.intersperse (Builder.byteString ", ") . fmap buildField . Boxed.toList
    in
      Builder.char8 '{' <> fields fs <> Builder.char8 '}'

  Union i r ->
    buildRow
      $ Struct
      $ Boxed.fromList [
        StructField (StructFieldName "tag") (Bytes i)
      , StructField (StructFieldName "value") r
      ]

  List rs ->
    let
      vals = mconcat . List.intersperse (Builder.byteString ", ") . fmap buildRow . Boxed.toList
    in
      Builder.char8 '[' <> vals rs <> Builder.char8 ']'

  Map rs ->
    let
      vals = mconcat . List.intersperse (Builder.byteString ", ") . fmap buildMap . Boxed.toList
    in
      Builder.char8 '{' <> vals rs <> Builder.char8 '}'

  Bool b ->
    bool_ b

  Bytes b ->
    Builder.word8Dec b

  Short b ->
    Builder.int16Dec b

  Integer b ->
    Builder.int32Dec b

  Long b ->
    Builder.int64Dec b

  Decimal b ->
    Builder.string8 $
      show b

  Date b ->
    Builder.char8 '"' <>
      builderUtf8_Ymd (Orc.dayToDate b) <>
        Builder.char8 '"'

  Timestamp b ->
    Builder.string8 $
      show b

  Float b ->
    Builder.floatDec b

  Double b ->
    Builder.doubleDec b

  String b ->
    quoteBytes b

  Char b ->
    quoteBytes b

  VarChar b ->
    quoteBytes b

  Binary b ->
    let
      vals = mconcat . List.intersperse (Builder.byteString ", ") . fmap Builder.word8Dec
    in
      Builder.char8 '[' <> vals (ByteString.unpack b) <> Builder.char8 ']'

  Partial mv ->
    maybe' (null_) buildRow mv


buildField :: StructField Row -> Builder.Builder
buildField (StructField (StructFieldName n) r) =
  text n <> colonSpace <> buildRow r


buildMap :: (Row,Row) -> Builder.Builder
buildMap (k, v) =
  buildRow k <> colonSpace <> buildRow v


colonSpace :: Builder
colonSpace = Prim.primBounded (ascii2 (':', ' ')) ()

-- | Encode a JSON null.
null_ :: Builder
null_ = Prim.primBounded (ascii4 ('n',('u',('l','l')))) ()

-- | Encode a JSON boolean.
bool_ :: Bool -> Builder
bool_ = Prim.primBounded (Prim.condB id (ascii4 ('t',('r',('u','e'))))
                                        (ascii5 ('f',('a',('l',('s','e'))))))



ascii2 :: (Char, Char) -> Prim.BoundedPrim a
ascii2 cs = Prim.liftFixedToBounded $ const cs >$< Prim.char7 >*< Prim.char7
{-# INLINE ascii2 #-}

ascii4 :: (Char, (Char, (Char, Char))) -> Prim.BoundedPrim a
ascii4 cs = Prim.liftFixedToBounded $ const cs >$<
    Prim.char7 >*< Prim.char7 >*< Prim.char7 >*< Prim.char7
{-# INLINE ascii4 #-}

ascii5 :: (Char, (Char, (Char, (Char, Char)))) -> Prim.BoundedPrim a
ascii5 cs = Prim.liftFixedToBounded $ const cs >$<
    Prim.char7 >*< Prim.char7 >*< Prim.char7 >*< Prim.char7 >*< Prim.char7
{-# INLINE ascii5 #-}

quoteBytes :: ByteString -> Builder
quoteBytes t = Builder.char8 '"' <> unquotedBytes t <> Builder.char8 '"'

-- | Encode a JSON string.
text :: Text -> Builder
text t = Builder.char8 '"' <> unquoted t <> Builder.char8 '"'

-- | Encode a JSON string, without enclosing quotes.
unquotedBytes :: ByteString -> Builder
unquotedBytes = Prim.primMapByteStringBounded escapeAscii

-- | Encode a JSON string, without enclosing quotes.
unquoted :: Text -> Builder
unquoted = encodeUtf8BuilderEscaped escapeAscii

escapeAscii :: Prim.BoundedPrim Word8
escapeAscii =
    Prim.condB (== c2w '\\'  ) (ascii2 ('\\','\\')) $
    Prim.condB (== c2w '\"'  ) (ascii2 ('\\','"' )) $
    Prim.condB (>= c2w '\x20') (Prim.liftFixedToBounded Prim.word8) $
    Prim.condB (== c2w '\n'  ) (ascii2 ('\\','n' )) $
    Prim.condB (== c2w '\r'  ) (ascii2 ('\\','r' )) $
    Prim.condB (== c2w '\t'  ) (ascii2 ('\\','t' )) $
    Prim.liftFixedToBounded hexEscape -- fallback for chars < 0x20
  where
    hexEscape :: Prim.FixedPrim Word8
    hexEscape = (\c -> ('\\', ('u', fromIntegral c))) >$<
        Prim.char8 >*< Prim.char8 >*< Prim.word16HexFixed
{-# INLINE escapeAscii #-}

c2w :: Char -> Word8
c2w c = fromIntegral (ord c)
{-# INLINE c2w #-}

-- | Given a 'Date' and a separator, construct a 'ByteString' 'BB.Builder'
--   corresponding to a Day\/Month\/Year encoding.
builderUtf8_Ymd :: Orc.Date -> Builder
builderUtf8_Ymd (Orc.Date y m d) =
       Builder.int64Dec y
    <> "-"
    <> pad2_ m
    <> "-"
    <> pad2_ d

-- | Encode a JSON boolean.
pad2_ :: Int64 -> Builder
pad2_ =
  Prim.primBounded
    (Prim.condB (<10)
      pad2from1
      (Prim.int64Dec))

  where
    pad2from1 :: Prim.BoundedPrim Int64
    pad2from1 =
      (\c -> ('0', c)) >$< Prim.liftFixedToBounded Prim.char7 >*< Prim.int64Dec
