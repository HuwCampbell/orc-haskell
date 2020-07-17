{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}

module Orc.Table.Convert (
    toLogical
  , streamLogical
) where

import           Control.Monad.State (runState, put, get)

import           Streaming (Of (..))
import qualified Streaming.Prelude as Streaming

import           Orc.Data.Data (StructField (..))
import           Orc.Data.Time (Day (..), Timestamp (..))
import           Orc.Schema.Types as Orc

import qualified Orc.Table.Striped as Striped
import qualified Orc.Table.Logical as Logical
import           Orc.X.Vector (safeHead)
import qualified Orc.X.Vector.Segment as Segment
import           Orc.X.Vector.Transpose (transpose)
import qualified Orc.Data.Time as Orc

import           Orc.Prelude

import           Data.ByteString (ByteString)
import qualified Data.Vector as Boxed
import           Data.Word (Word8)


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
          maybe boxed (\len -> Boxed.take (fromIntegral len) boxed) $ siNumberOfRows stripeInfo

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
        fmap (Logical.Date . Day) $
          boxed

    Striped.Timestamp seconds nanos ->
      let
        seconds_ =
          Boxed.convert seconds
        nanos_ =
          Boxed.convert nanos
      in
        Boxed.zipWith (Logical.Timestamp ... Timestamp) seconds_ nanos_

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
          maybe boxed (\len -> Boxed.take (fromIntegral len) boxed) $ siNumberOfRows stripeInfo

      in
        fmap Logical.Partial $
          fst $ flip runState 0 $
            Boxed.forM taken $ \here ->
              if here then do
                current <- get
                let
                  value = activeRows Boxed.! current
                put $ current + 1
                pure (Just' value)
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





fromLogical :: Boxed.Vector Logical.Row -> Maybe Striped.Column
fromLogical rows =
  case safeHead rows of
    Just (Logical.Bool _) ->
      Striped.Bool . Boxed.convert <$>
        traverse takeBool rows
    Just (Logical.Bytes _) ->
      Striped.Bytes . Boxed.convert <$>
        traverse takeBytes rows
    Just (Logical.Short _) ->
      Striped.Short . Boxed.convert <$>
        traverse takeShort rows
    Just (Logical.Integer _) ->
      Striped.Integer . Boxed.convert <$>
        traverse takeInteger rows
    Just (Logical.Long _) ->
      Striped.Long . Boxed.convert <$>
        traverse takeLong rows
    Just (Logical.Date _) ->
      Striped.Date . Boxed.convert <$>
        traverse takeDate rows
    Just (Logical.Float _) ->
      Striped.Float . Boxed.convert <$>
        traverse takeFloat rows
    Just (Logical.Double _) ->
      Striped.Double . Boxed.convert <$>
        traverse takeDouble rows

    Just (Logical.String _) ->
      Striped.String <$>
        traverse takeString rows

    Just (Logical.Struct ff) -> do
      rows_  <- traverse takeStruct rows
      cols   <- traverse fromLogical (transpose rows_)
      let
        names = fmap fieldName ff
      return $
        Striped.Struct $ Boxed.zipWith StructField names cols

    Just (Logical.List _) -> do
      rows_  <- traverse takeList rows
      let
        lens  = Boxed.convert $ fmap (fromIntegral . Boxed.length) rows_
      return $
        Striped.List lens undefined


takeString :: Logical.Row -> Maybe ByteString
takeString (Logical.String x) = Just x
takeString _                  = Nothing

takeBool :: Logical.Row -> Maybe Bool
takeBool (Logical.Bool x) = Just x
takeBool _                = Nothing

takeBytes :: Logical.Row -> Maybe Word8
takeBytes (Logical.Bytes x) = Just x
takeBytes _                 = Nothing

takeShort :: Logical.Row -> Maybe Int16
takeShort (Logical.Short x) = Just x
takeShort _                 = Nothing

takeInteger :: Logical.Row -> Maybe Int32
takeInteger (Logical.Integer x) = Just x
takeInteger _                   = Nothing

takeLong :: Logical.Row -> Maybe Int64
takeLong (Logical.Long x) = Just x
takeLong _                = Nothing

takeDate :: Logical.Row -> Maybe Int64
takeDate (Logical.Date (Orc.Day x)) = Just x
takeDate _                          = Nothing

takeFloat :: Logical.Row -> Maybe Float
takeFloat (Logical.Float x) = Just x
takeFloat _                 = Nothing

takeDouble :: Logical.Row -> Maybe Double
takeDouble (Logical.Double x) = Just x
takeDouble _                  = Nothing

takeStruct :: Logical.Row -> Maybe (Boxed.Vector Logical.Row)
takeStruct (Logical.Struct x) = Just (fmap fieldValue x)
takeStruct _                  = Nothing

takeList :: Logical.Row -> Maybe (Boxed.Vector Logical.Row)
takeList (Logical.List x) = Just x
takeList _                = Nothing


