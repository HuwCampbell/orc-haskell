{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}

module Orc.Table.Convert (
    toLogical
  , streamLogical

  , fromLogical
  , streamFromLogical
) where

import           Control.Monad.State (runState, put, get)
import           Control.Monad.Trans.Either (EitherT, hoistEither)

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Streaming.Prelude as Streaming

import           Orc.Data.Data (StructField (..))
import qualified Orc.Data.Time as Orc
import           Orc.Schema.Types as Orc

import qualified Orc.Table.Striped as Striped
import           Orc.Table.Logical as Logical

import           Orc.X.Vector (safeHead)
import qualified Orc.X.Vector.Segment as Segment
import           Orc.X.Vector.Transpose (transpose)

import           Orc.Prelude

import qualified Data.Decimal as Decimal
import           Data.String (String)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable


streamLogical
  :: Monad m
  => Streaming.Stream (Of Striped.Column) m x
  -> Streaming.Stream (Of Logical.Row) m x
streamLogical ss =
  Streaming.for ss $
    Streaming.each . toLogical


-- FIXME: This has failure states.
-- FIXME: This should return an either.
toLogical :: Striped.Column -> Boxed.Vector Logical.Row
toLogical =
  go
    where
  go = \case
    Striped.Struct cols ->
      let
        logicals =
          Boxed.map (go . fieldValue) cols

        transposed =
          transpose logicals

        asFields =
          Boxed.map (Boxed.zipWith (\s r -> s $> r) cols) transposed
      in
        fmap Logical.Struct asFields

    Striped.Map lengths keys values ->
      let
        key_ =
          go keys
        values_ =
          go values
        maps_ =
          Boxed.zipWith
            (Logical.Map ... Boxed.zip)
            (Segment.unsafeReify lengths key_)
            (Segment.unsafeReify lengths values_)

      in
        maps_

    Striped.List lengths col ->
      let
        inner = go col
      in
        fmap Logical.List $ Segment.unsafeReify lengths inner

    Striped.Bool bools ->
      let
        boxed =
          Boxed.convert bools

      in
        fmap Logical.Bool boxed

    Striped.Bytes x ->
      let
        boxed =
          Boxed.convert x
      in
        fmap Logical.Bytes boxed

    Striped.Short x ->
      let
        boxed =
          Boxed.convert x
      in
        fmap Logical.Short boxed

    Striped.Integer x ->
      let
        boxed =
          Boxed.convert x
      in
        fmap Logical.Integer boxed

    Striped.Long x ->
      let
        boxed =
          Boxed.convert x
      in
        fmap Logical.Long boxed

    Striped.Decimal integral scale ->
      let
        integral_ =
          Boxed.convert integral
        scale_ =
          Boxed.convert scale
        toDecimal i s =
          fromIntegral i / 10 ^^ s
      in
        Boxed.zipWith (Logical.Decimal ... toDecimal) integral_ scale_

    Striped.Date x ->
      let
        boxed =
          Boxed.convert x
      in
        fmap (Logical.Date . Orc.Day) $
          boxed

    Striped.Timestamp seconds nanos ->
      let
        seconds_ =
          Boxed.convert seconds
        nanos_ =
          Boxed.convert nanos
      in
        Boxed.zipWith (Logical.Timestamp ... Orc.Timestamp) seconds_ nanos_

    Striped.Float x ->
      let
        boxed =
          Boxed.convert x
      in
        fmap Logical.Float boxed

    Striped.Double x ->
      let
        boxed =
          Boxed.convert x
      in
        fmap Logical.Double boxed

    Striped.String x ->
      fmap Logical.String x

    Striped.Char x ->
      fmap Logical.Char x

    Striped.VarChar x ->
      fmap Logical.VarChar x

    Striped.Binary x ->
      fmap Logical.Binary x

    Striped.Partial present values ->
      let
        activeRows =
          go values

        boxed =
          Boxed.convert present

      in
        fmap Logical.Partial $
          fst $ flip runState 0 $
            Boxed.forM boxed $ \here ->
              if here then do
                current <- get
                let
                  partials = activeRows Boxed.! current
                put $ current + 1
                pure (Just' partials)
              else
                pure Nothing'

    Striped.Union tags variants ->
      let
        variantRows =
          Boxed.map go variants

        indicies =
          Boxed.map (const 0) variants

      in
        fmap (uncurry Logical.Union) $
          fst $ flip runState indicies $
            Boxed.forM (Boxed.convert tags) $ \tag -> do
              current <- get
              let
                rowIndex = current Boxed.! (fromIntegral tag)

              put $ current Boxed.// [(fromIntegral tag, rowIndex + 1)]
              pure $ (tag, (variantRows Boxed.! (fromIntegral tag)) Boxed.! rowIndex)



streamFromLogical
  :: Monad m
  => Int
  -> Type
  -> Streaming.Stream (Of Logical.Row) (EitherT String m) x
  -> Streaming.Stream (Of Striped.Column) (EitherT String m) x
streamFromLogical chunkSize schema =
  Streaming.mapM (hoistEither . fromLogical schema . Boxed.fromList) .
    Streaming.mapped (Streaming.toList) .
      Streaming.chunksOf chunkSize



fromLogical :: Type -> Boxed.Vector Logical.Row -> Either String Striped.Column
fromLogical schema rows =
  case safeHead rows of
    Just (Logical.Partial _) -> do
      partials <- note "Take Partials" $ traverse takePartials rows
      ms       <- fromLogical schema (Boxed.fromList $ catMaybes' $ Boxed.toList partials)
      let
        ps = fmap (isJust') partials
      return $
        Striped.Partial (Storable.convert ps) ms

    _ ->
      fromLogical' schema rows


fromLogical' :: Type -> Boxed.Vector Logical.Row -> Either String Striped.Column
fromLogical' schema rows =
  case schema of
    BOOLEAN ->
      note "Bool" $
      Striped.Bool . Boxed.convert <$>
        traverse takeBool rows
    BYTE ->
      note "Bytes" $
      Striped.Bytes . Boxed.convert <$>
        traverse takeBytes rows
    SHORT ->
      note "Short" $
      Striped.Short . Boxed.convert <$>
        traverse takeShort rows
    INT ->
      note "Integer" $
      Striped.Integer . Boxed.convert <$>
        traverse takeInteger rows
    LONG ->
      note "Long" $
      Striped.Long . Boxed.convert <$>
        traverse takeLong rows
    DATE ->
      note "Date" $
      Striped.Date . Boxed.convert <$>
        traverse takeDate rows
    FLOAT ->
      note "Float" $
      Striped.Float . Boxed.convert <$>
        traverse takeFloat rows
    DOUBLE ->
      note "Double" $
      Striped.Double . Boxed.convert <$>
        traverse takeDouble rows

    STRING ->
      note "String" $
      Striped.String <$>
        traverse takeString rows

    CHAR ->
      note "Char" $
      Striped.Char <$>
        traverse takeChar rows

    VARCHAR ->
      note "VarChar" $
      Striped.VarChar <$>
        traverse takeVarChar rows

    BINARY ->
      note "Binary" $
      Striped.Binary <$>
        traverse takeBinary rows

    STRUCT fts -> do
      rows_  <- note "Take Struct" $ traverse takeStruct rows
      let
        cols0 = transpose rows_
        colsX = Boxed.zip (Boxed.fromList $ fts) cols0

      cols   <- traverse (\(ft, c) -> traverse (flip fromLogical c) ft) colsX
      return $
        Striped.Struct cols

    LIST t -> do
      rows_  <- note "Take List" $ traverse takeList rows
      let
        lens  = Boxed.convert $ fmap (fromIntegral . Boxed.length) rows_
      ls0    <- fromLogical t (Boxed.concat (Boxed.toList rows_))
      return $
        Striped.List lens ls0

    MAP kt vt -> do
      rows_  <- note "Take Map" $ traverse takeMap rows
      let
        ks    = fmap (fmap fst) rows_
        vs    = fmap (fmap snd) rows_
        lens  = Boxed.convert $ fmap (fromIntegral . Boxed.length) rows_
      ks0    <- fromLogical kt (Boxed.concat (Boxed.toList ks))
      vs0    <- fromLogical vt (Boxed.concat (Boxed.toList vs))
      return $
        Striped.Map lens ks0 vs0

    UNION innerTypes -> do
      rows_  <- note "Take Union" $ traverse takeUnion rows
      let
        tags  = Boxed.convert $ fmap fst rows_

      inners <- ifor innerTypes $ \ix0 typ -> do
        let ix = fromIntegral ix0
        fromLogical typ $
          Boxed.mapMaybe (\(tag, row) -> if ix == tag then Just row else Nothing) rows_

      return $
        Striped.Union tags (Boxed.fromList inners)

    TIMESTAMP -> do
      rows_  <- note "Take Union" $ traverse takeTimestamp rows
      let
        unTS (Orc.Timestamp seconds nanos) = (seconds, nanos)

      return $
        uncurry Striped.Timestamp $
          bimap Boxed.convert Boxed.convert $
          Boxed.unzip $
          Boxed.map unTS $
            rows_

    DECIMAL -> do
      rows_  <- note "Take List" $ traverse takeDecimal rows
      let
        toStripeDecimal (Decimal.Decimal places mantissa) =
          let (n,p') = normalizePositive (mantissa, 0)
          in  (fromInteger n, fromInteger $ fromIntegral places - p')

      pure $
        uncurry Striped.Decimal $
          bimap Boxed.convert Boxed.convert $
          Boxed.unzip $
          Boxed.map toStripeDecimal $
            rows_


normalizePositive :: (Integer, Integer) -> (Integer, Integer)
normalizePositive (0, n) = (0, n)
normalizePositive (!c, !n) =
  case divMod c 10 of
    (c', r)
      | r  == 0   -> normalizePositive (c', n + 1)
      | otherwise -> (c, n)
