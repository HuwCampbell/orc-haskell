{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE DoAndIfThenElse     #-}
{-# LANGUAGE FlexibleContexts    #-}

module Orc.Table.Convert (
    streamLogical
  , streamSingle

  , fromLogical
  , streamFromLogical
) where

import           Control.Arrow ((&&&))
import           Control.Monad.Primitive (PrimMonad)
import           Control.Monad.Trans.Either (runEitherT, newEitherT, hoistEither)
import           Control.Monad.Except (MonadError, liftEither)

import           Streaming (Of (..))
import qualified Streaming as Streaming
import qualified Streaming.Internal as Streaming
import qualified Streaming.Prelude as Streaming

import qualified Orc.Data.Time as Orc
import           Orc.Schema.Types as Orc

import qualified Orc.Table.Striped as Striped
import           Orc.Table.Logical as Logical

import           Orc.X.Vector.Transpose (transpose)
import qualified Orc.X.Streaming as Streaming

import           Orc.Prelude

import qualified Data.Maybe as Maybe
import qualified Data.Scientific as Scientific
import           Data.String (String)
import qualified Data.Vector as Boxed
import qualified Data.Vector.Storable as Storable


-- | Stream rows from a stream of striped tables
streamLogical :: PrimMonad m => Streaming.Stream (Of Striped.Column) m r -> Streaming.Stream (Of Row) m r
streamLogical ss =
  Streaming.for ss
    streamSingle


-- | Stream rows from a striped table
streamSingle
  :: PrimMonad m
  => Striped.Column
  -> Streaming.Stream (Of Logical.Row) m ()
streamSingle col = case col of
  Striped.Bool x ->
    Streaming.map Logical.Bool (eachStorable x)

  Striped.Byte x ->
    Streaming.map Logical.Byte (eachStorable x)

  Striped.Short x ->
    Streaming.map Logical.Short (eachStorable x)

  Striped.Integer x ->
    Streaming.map Logical.Integer (eachStorable x)

  Striped.Long x ->
    Streaming.map Logical.Long (eachStorable x)

  Striped.Decimal x y ->
    let
      toDecimal i s =
        Logical.Decimal $
          Scientific.scientific (fromIntegral i) (negate (fromIntegral s))
    in
      Streaming.zipWith toDecimal (eachStorable x) (eachStorable y)

  Striped.Date bs ->
    Streaming.map (Logical.Date . Orc.Day) (eachStorable bs)

  Striped.Timestamp x y ->
    Streaming.zipWith (Logical.Timestamp ... Orc.Timestamp) (eachStorable x) (eachStorable y)

  Striped.Float x ->
    Streaming.map Logical.Float (eachStorable x)

  Striped.Double x ->
    Streaming.map Logical.Double (eachStorable x)

  Striped.String x ->
    Streaming.map Logical.String (Streaming.each x)

  Striped.Char x ->
    Streaming.map Logical.Char (Streaming.each x)

  Striped.VarChar x ->
    Streaming.map Logical.VarChar (Streaming.each x)

  Striped.Binary x ->
    Streaming.map Logical.Binary (Streaming.each x)


  Striped.Partial pBools inner ->
    flip Streaming.unfoldr (eachStorable pBools, streamSingle inner) $
      \(pS, iS) -> do
        x3 <- Streaming.next pS
        runEitherT $ do
          (present, pRest) <- hoistEither x3
          if present then do
            (iVal, iRest) <- newEitherT $ Streaming.next iS
            return $
              (iVal, (pRest, iRest))
          else
            return $
              (Null, (pRest, iS))


  Striped.Struct fs ->
    let
      fsStream0 = fmap (fmap streamSingle) fs

    in flip Streaming.unfoldr fsStream0 $ \fsStream -> do
      fsNexts <- traverse (traverse Streaming.next) fsStream

      return $ do
        gg <-
          traverse sequence fsNexts
        pure $
          (Logical.Struct . fmap (fmap fst) &&& fmap (fmap snd)) gg


  Striped.Union t xs ->
    flip Streaming.unfoldr (eachStorable t, fmap streamSingle xs) $
      \(tS, current) -> do
        x3 <- Streaming.next tS
        runEitherT $ do
          (tag, tagRest) <- hoistEither x3
          let
            rowIndex = current Boxed.! (fromIntegral tag)
          (iVal, iRest) <- newEitherT $ Streaming.next rowIndex

          return $
            (Logical.Union tag iVal, (tagRest, current Boxed.// [(fromIntegral tag, iRest)]))


  Striped.List l xs ->
    flip Streaming.unfoldr (eachStorable l, streamSingle xs) $
      \(lS, current) -> do
        eLen <- Streaming.next lS
        runEitherT $ do
          (len, lenRest)    <- hoistEither eLen
          let len0           = fromIntegral len
          (values :> xRest) <- Streaming.toVectorN len0 $ Streaming.splitAt len0 current
          return $
            (Logical.List values, (lenRest, xRest))


  Striped.Map l ks vs ->
    flip Streaming.unfoldr (eachStorable l, streamSingle ks, streamSingle vs) $
      \(lS, kCur, vCur) -> do
        eLen <- Streaming.next lS
        runEitherT $ do
          (len, lenRest)    <- hoistEither eLen
          let len0           = fromIntegral len
          (keys   :> kRest) <- Streaming.toVectorN len0 $ Streaming.splitAt len0 kCur
          (values :> vRest) <- Streaming.toVectorN len0 $ Streaming.splitAt len0 vCur
          return $
            (Logical.Map (Boxed.zip keys values), (lenRest, kRest, vRest))



eachStorable :: Storable.Storable a => Storable.Vector a -> Streaming.Stream (Of a) m ()
eachStorable =
  Storable.foldr
    (\a p -> Streaming.Step (a :> p))
    (Streaming.Return ())



streamFromLogical
  :: (Monad m, MonadError String m)
  => Int
  -- ^ Stripe size in number of rows
  -> Type
  -- ^ Type of records in the stream
  -> Streaming.Stream (Of Logical.Row) m x
  -- ^ Stream of typed records to write in ORC file
  -> Streaming.Stream (Of Striped.Column) m x
streamFromLogical chunkSize schema =
  Streaming.mapM (liftEither . fromLogical schema . Boxed.fromList) .
    Streaming.mapped (Streaming.toList) .
      Streaming.chunksOf chunkSize



fromLogical :: Type -> Boxed.Vector Logical.Row -> Either String Striped.Column
fromLogical schema rows = do
  let
    partials = fmap takePartials rows
  ms       <- fromLogical' schema (Boxed.mapMaybe id partials)
  let
    ps = fmap Maybe.isJust partials

  return $
    if Boxed.and ps then
      ms
    else
      Striped.Partial (Storable.convert ps) ms


fromLogical' :: Type -> Boxed.Vector Logical.Row -> Either String Striped.Column
fromLogical' schema rows =
  case schema of
    BOOLEAN ->
      note "Data corruption. Expected Bool" $
      Striped.Bool . Boxed.convert <$>
        traverse takeBool rows
    BYTE ->
      note "Data corruption. Expected Bytes" $
      Striped.Byte . Boxed.convert <$>
        traverse takeByte rows
    SHORT ->
      note "Data corruption. Expected Short" $
      Striped.Short . Boxed.convert <$>
        traverse takeShort rows
    INT ->
      note "Data corruption. Expected Integer" $
      Striped.Integer . Boxed.convert <$>
        traverse takeInteger rows
    LONG ->
      note "Data corruption. Expected Long" $
      Striped.Long . Boxed.convert <$>
        traverse takeLong rows
    DATE ->
      note "Data corruption. Expected Date" $
      Striped.Date . Boxed.convert <$>
        traverse takeDate rows
    FLOAT ->
      note "Data corruption. Expected Float" $
      Striped.Float . Boxed.convert <$>
        traverse takeFloat rows
    DOUBLE ->
      note "Data corruption. Expected Double" $
      Striped.Double . Boxed.convert <$>
        traverse takeDouble rows

    STRING ->
      note "Data corruption. Expected String" $
      Striped.String <$>
        traverse takeString rows

    CHAR ->
      note "Data corruption. Expected Char" $
      Striped.Char <$>
        traverse takeChar rows

    VARCHAR ->
      note "Data corruption. Expected VarChar" $
      Striped.VarChar <$>
        traverse takeVarChar rows

    BINARY ->
      note "Data corruption. Expected Binary" $
      Striped.Binary <$>
        traverse takeBinary rows

    STRUCT fts -> do
      rows_  <- note "Data corruption. Expected " $ traverse takeAnonymousStruct rows
      let
        vfts  = Boxed.fromList fts
        cols0 = transpose rows_
        colsX =
          if null cols0 then
            Boxed.map (\ft -> (ft, Boxed.empty)) vfts
          else
            Boxed.zip vfts cols0

      cols   <- traverse (\(ft, c) -> traverse (flip fromLogical c) ft) colsX
      return $
        Striped.Struct cols

    LIST t -> do
      rows_  <- note "Data corruption. Expected List" $ traverse takeList rows
      let
        lens  = Boxed.convert $ fmap (fromIntegral . Boxed.length) rows_
      ls0    <- fromLogical t (Boxed.concat (Boxed.toList rows_))
      return $
        Striped.List lens ls0

    MAP kt vt -> do
      rows_  <- note "Data corruption. Expected Map" $ traverse takeMap rows
      let
        ks    = fmap (fmap fst) rows_
        vs    = fmap (fmap snd) rows_
        lens  = Boxed.convert $ fmap (fromIntegral . Boxed.length) rows_
      ks0    <- fromLogical kt (Boxed.concat (Boxed.toList ks))
      vs0    <- fromLogical vt (Boxed.concat (Boxed.toList vs))
      return $
        Striped.Map lens ks0 vs0

    UNION innerTypes -> do
      rows_  <- note "Data corruption. Expected Union" $ traverse takeUnion rows
      let
        tags  = Boxed.convert $ fmap fst rows_

      inners <- ifor innerTypes $ \ix0 typ -> do
        let ix = fromIntegral ix0
        fromLogical typ $
          Boxed.mapMaybe (\(tag, row) -> if ix == tag then Just row else Nothing) rows_

      return $
        Striped.Union tags (Boxed.fromList inners)

    TIMESTAMP -> do
      rows_  <- note "Data corruption. Expected Union" $ traverse takeTimestamp rows
      let
        unTS (Orc.Timestamp seconds nanos) = (seconds, nanos)

      return $
        uncurry Striped.Timestamp $
          bimap Boxed.convert Boxed.convert $
          Boxed.unzip $
          Boxed.map unTS $
            rows_

    DECIMAL -> do
      rows_  <- note "Data corruption. Expected List" $ traverse takeDecimal rows
      let
        toStripeDecimal scientific =
          (fromInteger $ Scientific.coefficient scientific, negate (fromIntegral $ Scientific.base10Exponent scientific))

      pure $
        uncurry Striped.Decimal $
          bimap Boxed.convert Boxed.convert $
          Boxed.unzip $
          Boxed.map toStripeDecimal $
            rows_
