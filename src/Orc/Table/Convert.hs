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

import           Data.String (String)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable


streamLogical
  :: Monad m
  => Streaming.Stream (Of (StripeInformation, Striped.Column)) m x
  -> Streaming.Stream (Of Logical.Row) m x
streamLogical ss =
  Streaming.for ss $
    Streaming.each . uncurry toLogical


toLogical :: StripeInformation -> Striped.Column -> Boxed.Vector Logical.Row
toLogical stripeInfo =
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
          Boxed.map (Boxed.zipWith (\(StructField n _) r -> StructField n r) cols) transposed

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

        taken =
          maybe boxed (\len -> Boxed.take (fromIntegral len) boxed) (siNumberOfRows stripeInfo)

      in
        fmap Logical.Bool taken

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

        taken =
          maybe boxed (\len -> Boxed.take (fromIntegral len) boxed) (siNumberOfRows stripeInfo)

      in
        fmap Logical.Partial $
          fst $ flip runState 0 $
            Boxed.forM taken $ \here ->
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
        colsX = Boxed.zip (Boxed.fromList $ fmap fieldValue fts) cols0
        names = Boxed.fromList (fmap fieldName fts)

      cols   <- traverse (uncurry fromLogical) colsX

      return $
        Striped.Struct $ Boxed.zipWith StructField names cols

    LIST t -> do
      rows_  <- note "Take List" $ traverse takeList rows
      let
        lens  = Boxed.convert $ fmap (fromIntegral . Boxed.length) rows_
      ls0    <- traverse (fromLogical t) rows_ <|> pure (Boxed.empty)
      ls1    <- Striped.concat (toList ls0)
      return $
        Striped.List lens ls1

    MAP kt vt -> do
      rows_  <- note "Take Map" $ traverse takeMap rows
      let
        ks    = fmap (fmap fst) rows_
        vs    = fmap (fmap snd) rows_
        lens  = Boxed.convert $ fmap (fromIntegral . Boxed.length) rows_
      ks0    <- traverse (fromLogical kt) ks
      ks1    <- Striped.concat (toList ks0)
      vs0    <- traverse (fromLogical vt) vs
      vs1    <- Striped.concat (toList vs0)
      return $
        Striped.Map lens ks1 vs1

    UNION _ -> do
      _rows_  <- note "Take Union" $ traverse takeUnion rows
      Left "Not finished UNION"

    TIMESTAMP ->
      Left "Not finished TIMESTAMP"

    DECIMAL -> do
      Left "Not finished DECIMAL"
