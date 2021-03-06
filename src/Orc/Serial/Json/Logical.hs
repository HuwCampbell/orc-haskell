{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{- | Serialize a Logical Row to a JSON value.

     This doesn't use Aeson or another intermediate JSON data type
     and instead goes directly to a Builder / Lazy ByteString for
     two reasons.
       - The Order of columns would come out in a random order, due
         to Aeson using a HashMap for its objects
       - Performance reasons; this already takes up 50% of the time
         in printing an Orc file to Json, so we don't want it to be
         any slower.

-}
module Orc.Serial.Json.Logical (
  ppJsonRow
) where

import           Orc.Prelude

import           Control.Monad.IO.Class (MonadIO)
import qualified Streaming.ByteString as Streaming

import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString
import           Data.Char (ord)
import           Data.Word (Word8)
import qualified Data.List as List
import           Data.ByteString.Builder (Builder)
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Builder.Prim as Prim
import           Data.ByteString.Builder.Prim ((>*<), (>$<))
import qualified Data.Vector as Boxed
import           Data.Text.Encoding (encodeUtf8BuilderEscaped)

import           Orc.Data.Data
import qualified Orc.Data.Time as Orc
import           Orc.Table.Logical (Row (..))


ppJsonRow :: MonadIO m => Row -> Streaming.ByteStream m ()
ppJsonRow =
  Streaming.toStreamingByteString . (<> Builder.char8 '\n') . buildRow


-- | Render a row json.
buildRow :: Row -> Builder.Builder
buildRow = \case
  Struct fs ->
    let
      fields = mconcat . List.intersperse commaSpace . fmap buildField . Boxed.toList
    in
      Builder.char8 '{' <> fields fs <> Builder.char8 '}'

  Union i r ->
    buildRow
      $ Struct
      $ Boxed.fromList [
        StructField (StructFieldName "tag") (Byte i)
      , StructField (StructFieldName "value") r
      ]

  List rs ->
    let
      vals = mconcat . List.intersperse commaSpace . fmap buildRow . Boxed.toList
    in
      Builder.char8 '[' <> vals rs <> Builder.char8 ']'

  Map rs ->
    let
      vals = mconcat . List.intersperse commaSpace . fmap buildMap . Boxed.toList
    in
      Builder.char8 '[' <> vals rs <> Builder.char8 ']'

  Bool b ->
    bool_ b

  Byte b ->
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
    Builder.char8 '"' <>
      builderDateTime (Orc.timestampToDateTime b)  <>
        Builder.char8 '"'


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
      vals = mconcat . List.intersperse commaSpace . fmap Builder.word8Dec
    in
      Builder.char8 '[' <> vals (ByteString.unpack b) <> Builder.char8 ']'

  Null ->
    null_

-- | Render a struct field as an object part.
buildField :: StructField Row -> Builder.Builder
buildField (StructField (StructFieldName n) r) =
  text n <> colonSpace <> buildRow r


-- | Render a Map as a struct with Key and Value fields.
--
-- It would be nice to use a json assoc, but there's no
-- guarantee the key is stringly.
buildMap :: (Row,Row) -> Builder.Builder
buildMap (k, v) =
  buildRow
    $ Struct
    $ Boxed.fromList [
      StructField (StructFieldName "key") k
    , StructField (StructFieldName "value") v
    ]


commaSpace :: Builder
commaSpace = Prim.primBounded (ascii2 (',', ' ')) ()


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


builderDateTime :: Orc.DateTime -> Builder
builderDateTime (Orc.DateTime d ns) =
  builderUtf8_Ymd d <> "T" <> builderUtf8_HMS ns


builderUtf8_HMS :: Word64 -> Builder
builderUtf8_HMS nanos =
       pad2_ (fromIntegral h)
    <> ":"
    <> pad2_ (fromIntegral m)
    <> ":"
    <> pad2_ (fromIntegral s)
    <> "."
    <> lengthWord64 ns
  where
    (h, mm) = nanos `divMod` 3600000000000
    (m, ss) = mm    `divMod`   60000000000
    (s, ns) = ss    `divMod`    1000000000


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


-- | Render nanoseconds, padding to a decimal point
lengthWord64 :: Word64 -> Builder
lengthWord64 n
  | n < 10 = "00000000" <> Builder.word64Dec n
  | n < 100 = "0000000" <> Builder.word64Dec n
  | n < 1000 = "000000" <> Builder.word64Dec n
  | n < 10000 = "00000" <> Builder.word64Dec n
  | n < 100000 = "0000" <> Builder.word64Dec n
  | n < 1000000 = "000" <> Builder.word64Dec n
  | n < 10000000 = "00" <> Builder.word64Dec n
  | n < 100000000 = "0" <> Builder.word64Dec n
  | otherwise      =       Builder.word64Dec n
