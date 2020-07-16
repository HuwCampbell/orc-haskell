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
import           Orc.Data.Time (Day (..))
import           Orc.Schema.Types as Orc

import qualified Orc.Table.Striped as Striped
import           Orc.Table.Logical as Logical
import           Orc.X.Vector.Segment as Segment
import           Orc.X.Vector.Transpose (transpose)

import           Orc.Prelude

import qualified Data.Vector as Boxed


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
        fmap Logical.Date $
          Boxed.map Day $
            boxed

    Striped.Timestamp x ->
      let
        boxed =
          Boxed.convert x
      in
        fmap Logical.Timestamp boxed

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



(...) :: (c -> d) -> (a -> b -> c) -> a -> b -> d
(...) = (.) . (.)
