{-# LANGUAGE BangPatterns             #-}
{-# LANGUAGE NoImplicitPrelude        #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE LambdaCase               #-}
{-# LANGUAGE PatternGuards            #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE FlexibleContexts         #-}

module Orc.Encodings.Integers (
    decodeFloat32
  , decodeFloat64

  , decodeIntegerRLEv1
  , decodeBase128Varint

  , getBase128Varint
  , putBase128Varint

  , getIntegerRLEv1
  , putIntegerRLEv1

  , getIntegerRLEv2
  , decodeIntegerRLEv2
) where

import           Control.Monad.ST (runST)
import qualified Data.STRef as ST

import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get

import           Data.Serialize.Put (Putter)
import qualified Data.Serialize.Put as Put

import qualified Data.Serialize.IEEE754 as Get
import           Data.Bits ((.&.), (.|.), complement, shiftL, shiftR)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as ByteString
import           Data.Word (Word8, Word64)
import           Data.String (String)
import           Data.Ratio ((%))

import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Storable.Mutable as Mutable

import           Foreign (Storable (..), mallocForeignPtrArray, withForeignPtr, plusPtr)
import           Foreign.Ptr (Ptr)

import           Orc.Encodings.Primitives

import           System.IO
import           System.IO.Unsafe (unsafePerformIO)

import           P


b10000000 :: Word8
b10000000 = 0x80

b01111111 :: Word8
b01111111 = complement 0x80

manyStorable :: (Storable.Storable a, Alternative f, Monad f) => f a -> f (Storable.Vector a)
manyStorable from =
  Storable.unfoldrM (\_ ->
    ((\a -> Just (a, ())) <$> from) <|> pure Nothing
  ) ()

{-# INLINE decodeBase128Varint #-}
decodeBase128Varint :: ByteString ->  Either String (Storable.Vector Word64)
decodeBase128Varint bytes =
  Get.runGet (manyStorable getBase128Varint) bytes


{-# INLINE decodeFloat32 #-}
decodeFloat32 :: ByteString ->  Either String (Storable.Vector Float)
decodeFloat32 bytes =
  Get.runGet (manyStorable Get.getFloat32le) bytes


{-# INLINE decodeFloat64 #-}
decodeFloat64 :: ByteString ->  Either String (Storable.Vector Double)
decodeFloat64 bytes =
  Get.runGet (manyStorable Get.getFloat64le) bytes


{-# INLINE getBase128Varint #-}
getBase128Varint :: forall w. OrcNum w => Get w
getBase128Varint =
  let
    go :: OrcWord w -> Int -> Get w
    go !accumulator !shift = do
      byte <- Get.getWord8
      let
        masked =
          byte .&. b01111111

        new =
          (fromIntegral masked `shiftL` shift) .|. accumulator

        terminates =
          (byte .&. b10000000) /= b10000000

      if terminates
        then return (unZigZag new)
        else go new (7 + shift)
  in
    go 0 0


{-# INLINE putBase128Varint #-}
putBase128Varint :: forall w. OrcNum w => Putter w
putBase128Varint =
  let
    go chunk = do
      let
        masked =
          fromIntegral chunk .&. b01111111

        remainder =
          chunk `shiftR` 7

        terminates =
          remainder == 0

      if terminates
        then Put.putWord8 masked
        else Put.putWord8 (masked .|. b10000000) >> go remainder
  in
    go . zigZag


-- {-# INLINABLE decodeWord64 #-}
-- decodeWord64 :: ByteString -> Either String (Storable.Vector Word64)
-- decodeWord64 = decodeBase128Varint


-- {-# INLINABLE decodeInt64 #-}
-- decodeInt64 :: ByteString -> Either String (Storable.Vector Int64)
-- decodeInt64 bytes =
--   let
--     words = decodeWord64 bytes
--   in
--     fmap (Storable.map unZigZag64) words


{-# INLINE decodeIntegerRLEv1 #-}
decodeIntegerRLEv1 :: forall w . (Storable w, OrcNum w) => ByteString -> Either String (Storable.Vector w)
decodeIntegerRLEv1 bytes =
  Get.runGet getIntegerRLEv1 bytes


{-# INLINE getIntegerRLEv1 #-}
getIntegerRLEv1 :: forall w . (Storable w, OrcNum w) => Get (Storable.Vector w)
getIntegerRLEv1 =
  let
    getSet :: Get (Storable.Vector w)
    getSet = do
      header <- Get.getInt8
      if header >= 0
        then do
          let
            runLength :: Int
            runLength = fromIntegral header + 3;

          delta   <- Get.getInt8
          initial <- getBase128Varint
          return $
            Storable.enumFromStepN initial (fromIntegral delta) runLength

        else do
          let
            listLength = negate $ fromIntegral header
          Storable.replicateM listLength getBase128Varint

  in
    Storable.concat <$>
      many getSet



putIntegerRLEv1 :: forall w . (Storable w, OrcNum w) => Putter (Storable.Vector w)
putIntegerRLEv1 =
  let
    toRuns :: Storable.Vector w -> [(w, Word8)]
    toRuns =
      let
        collect x ((y, n):xs0)
          | x == y
          , n < 130
          = ((y, n + 1):xs0)
        collect x xs
          = (x, 1) : xs
      in
        Storable.foldr collect []


    takeLiterals :: [(w, Word8)] -> ([(w, Word8)], [(w, Word8)])
    takeLiterals =
      let
        go :: Word8 -> [(w, Word8)] -> ([(w, Word8)], [(w, Word8)])
        go n rest
          | (x, i) : xs <- rest
          , i < 3
          , n + i < 128
          = let (r, rs) = go (n + i) xs
            in  ((x,i):r, rs)
          | otherwise
          = ([], rest)

      in go 0

    putSet :: Putter (Storable.Vector w)
    putSet words =
      let
        runs = toRuns words

        place []
          = pure ()
        place ws@((w, n):ws0)
          | n >= 3
          = do Put.putWord8 (n - 3)
               Put.putWord8 0
               putBase128Varint w
               place ws0

          | otherwise
          = let
              (noRuns, runStart) =
                takeLiterals ws

              totalLen =
                sum $ snd <$> noRuns

              header =
                negate . fromIntegral $ totalLen

            in do Put.putInt8 header
                  for_ noRuns $
                    \(v,i) ->
                      for_ [1.. i] $
                        const (putBase128Varint v)

                  place runStart

      in
        place runs
  in
    putSet



{-# INLINE decodeIntegerRLEv2 #-}
decodeIntegerRLEv2 :: forall w . (Storable w, OrcNum w) => ByteString ->  Either String (Storable.Vector w)
decodeIntegerRLEv2 =
  Get.runGet getIntegerRLEv2


{-# INLINE getIntegerRLEv2 #-}
getIntegerRLEv2 :: forall w . (Storable w, OrcNum w) => Get (Storable.Vector w)
getIntegerRLEv2 =
  let
    ensureEmpty =
      Get.isEmpty >>= \case
        True -> return []
        False -> mzero

    consumeMany a =
      ensureEmpty <|> consumeSome a

    consumeSome a =
      (:) <$> a <*> consumeMany a

    getSet :: Get (Storable.Vector w)
    getSet = do
      opening  <- Get.lookAhead Get.getWord8
      case opening `shiftR` 6 of
        0 ->
          getShortRepeat

        1 ->
          getDirect

        2 ->
          getPatchedBase

        3 ->
          getDelta

        _ ->
          fail "Impossible!"

  in
    Storable.concat <$>
      consumeMany getSet


{-# INLINE getShortRepeat #-}
getShortRepeat :: forall w . (Storable w, OrcNum w) => Get (Storable.Vector w)
getShortRepeat = do
  header <- Get.getWord8
  let
    width =
      (header `shiftR` 3) + 1
    repeats =
      (header .&. 0x07) + 3
  value <-
    unZigZag . fromIntegral <$>
      getWordBe width
  return $
    Storable.replicate (fromIntegral repeats) value


{-# INLINE getDirect #-}
getDirect :: forall w . (Storable w, OrcNum w) => Get (Storable.Vector w)
getDirect = do
  header <- Get.getWord16be
  let
    width =
      bitSizeLookup $
        fromIntegral $
          (header .&. 0x3E00) `shiftR` 9

    repeats =
      fromIntegral $
        (header .&. 0x01FF) + 1

    required =
      repeats * width

  dataBytes <- Get.getByteString (ceiling (required % 8))

  return $
    Storable.map (unZigZag . fromIntegral) $
      readLongsNative dataBytes repeats width


{-# INLINE getPatchedBase #-}
getPatchedBase :: forall w . (Storable w, OrcNum w) => Get (Storable.Vector w)
getPatchedBase = do
  header <- Get.getWord32be
  let
    width =
      bitSizeLookup $
        fromIntegral $
          (header .&. 0x3E000000) `shiftR` 25

    repeats =
      fromIntegral $
        ((header .&. 0x01FF0000) `shiftR` 16) + 1

    baseWidth =
      fromIntegral $
        ((header .&. 0x000E000) `shiftR` 13) + 1

    patchWidth =
      bitSizeLookup $
        fromIntegral $
          (header .&. 0x0001F00) `shiftR` 8

    patchGapWidth =
      fromIntegral $
        ((header .&. 0x00000E0) `shiftR` 5) + 1

    patchListLength =
      fromIntegral $
        header .&. 0x000001F

  baseValue <-
    unZigZag . fromIntegral <$>
      getWordBe baseWidth

  dataBytes <-
    Get.getByteString (ceiling ((repeats * width) % 8))

  patchBytes <-
    Get.getByteString (ceiling ((patchListLength * (patchWidth + patchGapWidth)) % 8))

  let
    unadjustedValues =
      readLongsNative dataBytes repeats width

    patchGapsAndValues =
      readLongsNative patchBytes patchListLength (patchWidth + patchGapWidth)

    patchedValues =
      runST $ do
        working <- Storable.unsafeThaw unadjustedValues
        index   <- ST.newSTRef 0
        Storable.forM_ patchGapsAndValues $ \patch -> do
          let
            gap
              = patch `shiftR` fromIntegral patchWidth

            restGap =
              fromIntegral $
                64 - patchWidth

            diff
              = ((patch `shiftL` restGap) `shiftR` restGap) `shiftL` fromIntegral width

          ST.modifySTRef index (+ (fromIntegral gap))
          Mutable.modify working (.|. diff) =<< ST.readSTRef index
        Storable.unsafeFreeze working

    adjustedValue =
      Storable.map ((+ baseValue) . fromIntegral) patchedValues

  return
    adjustedValue


{-# INLINE getDelta #-}
getDelta :: forall w . (Storable w, OrcNum w) => Get (Storable.Vector w)
getDelta = do
  header <- Get.getWord16be
  let
    width =
      bitSizeLookup $
        fromIntegral $
          (header .&. 0x3E00) `shiftR` 9

    repeats =
      fromIntegral $
        (header .&. 0x01FF) + 1

    deltaRepeats =
      repeats - 2

    required =
      deltaRepeats * width

  baseValue <-
    getBase128Varint

  deltaBase <-
    fromIntegral . unZigZag64 <$> getBase128Varint

  deltaBytes <-
    Get.getByteString (ceiling (required % 8))

  let
    deltas =
      Storable.map (unZigZag . fromIntegral) $
        readLongsNative deltaBytes deltaRepeats width

  return $
    Storable.scanl' (+) baseValue (Storable.singleton deltaBase <> deltas)

{-# INLINE readLongsNative #-}
readLongsNative :: ByteString -> Word64 -> Word64 -> Storable.Vector Word64
readLongsNative bytes len bitsize =
  unsafePerformIO $ do
    let
      (inPtr, offset, _inLen) =
        ByteString.toForeignPtr bytes

    outPtr <- mallocForeignPtrArray (fromIntegral len)

    withForeignPtr inPtr $ \inPtr' ->
      withForeignPtr outPtr $ \outPtr' ->
        readLongs (plusPtr inPtr' offset) len bitsize outPtr'

    return $
      Storable.unsafeFromForeignPtr outPtr 0 (fromIntegral len)

foreign import ccall unsafe
  readLongs
    :: Ptr Word8 -> Word64 -> Word64 -> Ptr Word64 -> IO ()


bitSizeLookup :: Int -> Word64
bitSizeLookup =
  let
    table =
      Storable.fromList [
        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        26, 28, 30, 32, 40, 48, 56, 64
      ]
  in
    \key ->
      table Storable.! fromIntegral key
