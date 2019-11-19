{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}

module Orc.Serial.Logical (
    toLogical

  , streamLogical
) where


import           Control.Monad.IO.Class
import           Control.Monad.Except (MonadError, liftEither, throwError)
import           Control.Monad.State (MonadState (..), StateT (..), evalStateT, modify')
import           Control.Monad.Reader (ReaderT (..), runReaderT, ask)
import           Control.Monad.Trans.Class (lift)
import           Control.Monad.Trans.Control (MonadTransControl (..))
import           Control.Monad.Trans.Either (EitherT, newEitherT, left)
import           Control.Monad.Trans.Resource (MonadResource (..), allocate)

import qualified Data.Serialize.Get as Get

import           Data.List (dropWhile)
import           Data.Word (Word64)

import           Data.String (String)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as ByteString

import qualified Data.Vector.Storable as Storable


import           Viking (Of (..), ByteStream)
import qualified Viking.Stream as Viking
import qualified Viking.ByteStream as ByteStream

import           Orc.Data.Segmented
import           Orc.Data.Data (StructField (..), Indexed, currentIndex, currentValue, nextIndex, makeIndexed, prevIndex)
import           Orc.Schema.Types as Orc
import           Orc.Serial.Encodings.Bytes
import           Orc.Serial.Encodings.Compression
import           Orc.Serial.Encodings.Integers

import qualified Orc.Table.Striped as Striped
import           Orc.Table.Logical as Logical
import           Orc.X.Vector.Segment as Segment

import           System.IO as IO

import           P

import qualified X.Data.Vector as Boxed

streamLogical
  :: Monad m
  => Viking.Stream (Of (StripeInformation, Striped.Column)) m x
  -> Viking.Stream (Of Logical.Row) m x
streamLogical stripes =
  Viking.for stripes $
    Viking.each . uncurry toLogical


toLogical :: StripeInformation -> Striped.Column -> Boxed.Vector Logical.Row
toLogical stripeInfo column =
  case column of
    Striped.Struct cols ->
      let
        logicals =
          Boxed.map (toLogical stripeInfo . fieldValue) cols

        transposed =
          Boxed.transpose logicals

        asFields =
          Boxed.map (Boxed.zipWith (\(StructField n _) r -> StructField n r) cols) transposed

      in
        fmap Logical.Struct asFields

    Striped.Map lengths keys values ->
      let
        key_ =
          toLogical stripeInfo keys
        values_ =
          toLogical stripeInfo values
        maps_ =
          Boxed.zipWith
            (Logical.Map ... Boxed.zip)
            (Segment.unsafeReify lengths key_)
            (Segment.unsafeReify lengths values_)

      in
        maps_

    Striped.List lengths col ->
      let
        inner = toLogical stripeInfo col
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
        fmap Logical.Date boxed

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

    -- Actually implement this.
    Striped.Partial _ ha ->
      toLogical stripeInfo ha


(...) :: (c -> d) -> (a -> b -> c) -> a -> b -> d
(...) = (.) . (.)
