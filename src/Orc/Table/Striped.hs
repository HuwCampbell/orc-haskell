{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE DoAndIfThenElse   #-}

module Orc.Table.Striped (
    Column (..)

  , append
  , concat
  , length
) where


import           Control.Monad (foldM)

import           Data.ByteString (ByteString)
import           Data.String (String)
import           Data.Word (Word8, Word32)
import           Data.WideWord (Int128)

import           Orc.Data.Data
import           Orc.Schema.Types

import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable

import           Orc.Prelude hiding (length)
import           Orc.X.Vector

data Column
  -- Composite Columns
  = Struct    !(Boxed.Vector (StructField Column))
  | Union     !(Storable.Vector Word8) !(Boxed.Vector Column)
  | List      !(Storable.Vector Word32) !Column
  | Map       !(Storable.Vector Word32) !Column !Column

  -- Primitive Columns
  | Bool      !(Storable.Vector Bool)
  | Byte      !(Storable.Vector Word8)

  | Short     !(Storable.Vector Int16)
  | Integer   !(Storable.Vector Int32)
  | Long      !(Storable.Vector Int64)

  | Decimal   !(Storable.Vector Int128) !(Storable.Vector Int8)
  | Date      !(Storable.Vector Int64)
  | Timestamp !(Storable.Vector Int64)  !(Storable.Vector Word64)

  | Float     !(Storable.Vector Float)
  | Double    !(Storable.Vector Double)

  | String    !(Boxed.Vector ByteString)
  | Char      !(Boxed.Vector ByteString)
  | VarChar   !(Boxed.Vector ByteString)

  | Binary    !(Boxed.Vector ByteString)

  -- For Nullable columns.
  | Partial   !(Storable.Vector Bool) !Column
  deriving (Eq, Show)



length :: Column -> Either String Int
length = \case
  Struct cols ->
    case safeHead cols of
      Just col ->
        length (fieldValue col)
      Nothing  ->
        Left "Can't determine length of Struct with no columns"

  Union t _ ->
    Right $ Storable.length t

  List ls _ ->
    Right $ Storable.length ls

  Map ls _ _ -> do
    Right $ Storable.length ls

  Bool x ->
    Right $ Storable.length x

  Byte x ->
    Right $ Storable.length x

  Short x ->
    Right $ Storable.length x

  Integer x ->
    Right $ Storable.length x

  Long x ->
    Right $ Storable.length x

  Decimal x _ ->
    Right $ Storable.length x

  Date x ->
    Right $ Storable.length x

  Timestamp x _ ->
    Right $ Storable.length x

  Float x ->
    Right $ Storable.length x

  Double x ->
    Right $ Storable.length x

  String x ->
    Right $ Boxed.length x

  Char x ->
    Right $ Boxed.length x

  VarChar x ->
    Right $ Boxed.length x

  Binary x ->
    Right $ Boxed.length x

  Partial x _ ->
    Right $ Storable.length x


concat :: Type -> [Column] -> Either String Column
concat typ xs  = case xs of
  (x:xss) ->
    foldM append x xss
  _ ->
    Right (emptyOf typ)


emptyOf :: Type -> Column
emptyOf = \case
  BOOLEAN ->
    Bool Storable.empty
  BYTE ->
    Byte Storable.empty
  SHORT ->
    Short Storable.empty
  INT ->
    Integer Storable.empty
  LONG ->
    Long Storable.empty
  FLOAT ->
    Float Storable.empty
  DOUBLE ->
    Double Storable.empty
  STRING ->
    String Boxed.empty
  BINARY ->
    Binary Boxed.empty
  DATE ->
    Date Storable.empty
  VARCHAR ->
    VarChar Boxed.empty
  CHAR ->
    Char Boxed.empty
  TIMESTAMP ->
    Timestamp Storable.empty Storable.empty
  LIST it ->
    List Storable.empty (emptyOf it)
  MAP kt vt ->
    Map Storable.empty (emptyOf kt) (emptyOf vt)
  STRUCT kts ->
    Struct $ Boxed.fromList (fmap (fmap emptyOf) kts)
  UNION uts ->
    Union Storable.empty $ Boxed.fromList (fmap emptyOf uts)
  DECIMAL ->
    Decimal Storable.empty Storable.empty



append :: Column -> Column -> Either String Column
append c0 c1 =
  case (c0, c1) of
    (Struct fs0, Struct fs1) ->
      Struct <$> Boxed.zipWithM unsafeAppendField fs0 fs1

    (Union tags0 fs0, Union tags1 fs1) ->
      Union (tags0 <> tags1) <$> Boxed.zipWithM append fs0 fs1

    (List len0 fs0, List len1 fs1) ->
      List (len0 <> len1) <$> append fs0 fs1

    (Map len0 k0 v0, Map len1 k1 v1) ->
      Map (len0 <> len1) <$> append k0 k1 <*> append v0 v1

    (Bool x0, Bool x1) ->
      Right $ Bool (x0 <> x1)

    (Byte x0, Byte x1) ->
      Right $ Byte (x0 <> x1)

    (Short x0, Short x1) ->
      Right $ Short (x0 <> x1)

    (Integer x0, Integer x1) ->
      Right $ Integer (x0 <> x1)

    (Long x0, Long x1) ->
      Right $ Long (x0 <> x1)

    (Decimal m0 x0, Decimal m1 x1) ->
      Right $ Decimal (m0 <> m1) (x0 <> x1)

    (Date x0, Date x1) ->
      Right $ Date (x0 <> x1)

    (Timestamp x0 y0, Timestamp x1 y1) ->
      Right $ Timestamp (x0 <> x1) (y0 <> y1)

    (Float x0, Float x1) ->
      Right $ Float (x0 <> x1)

    (Double x0, Double x1) ->
      Right $ Double (x0 <> x1)

    (String x0, String x1) ->
      Right $ String (x0 <> x1)

    (Char x0, Char x1) ->
      Right $ Char (x0 <> x1)

    (VarChar x0, VarChar x1) ->
      Right $ VarChar (x0 <> x1)

    (Binary x0, Binary x1) ->
      Right $ Binary (x0 <> x1)

    (Partial x0 y0, Partial x1 y1) ->
      Partial (x0 <> x1) <$> append y0 y1

    _ ->
      Left "Mismatched Columns"


unsafeAppendField :: StructField Column -> StructField Column -> Either String (StructField Column)
unsafeAppendField f0 f1 =
  if fieldName f0 == fieldName f1 then
    (f0 $>) <$> append (fieldValue f0) (fieldValue f1)
  else
    Left "Mismatched Column names"
{-# INLINABLE unsafeAppendField #-}
