{-# LANGUAGE BangPatterns             #-}
{-# LANGUAGE CPP                      #-}
{-# LANGUAGE DoAndIfThenElse          #-}
{-# LANGUAGE NoImplicitPrelude        #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE LambdaCase               #-}
{-# LANGUAGE PatternGuards            #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE TypeApplications         #-}

module Orc.Serial.Binary.Internal.Integers (
    decodeIntegerRLEv1
  , decodeBase128Varint

  , getBase128Varint
  , putBase128Varint

  , getIntegerRLEv1
  , putIntegerRLEv1

  , getIntegerRLEv2
  , decodeIntegerRLEv2

  , decodeNanoseconds
  , encodeNanoseconds
) where

import           Control.Arrow ((&&&))
import           Control.Monad.ST (runST)
import qualified Data.STRef as ST

import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get

import           Data.Serialize.Put (Putter)
import qualified Data.Serialize.Put as Put

import           Data.Bits ((.&.), (.|.), complement, shiftL, shiftR, testBit)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as ByteString
import           Data.Word (Word8, Word16, Word32)
import           Data.WideWord (Int128, Word128)
import           Data.String (String)

import qualified Data.Vector.Storable as Storable
import qualified Data.Vector.Storable.Mutable as Mutable

import           Foreign (mallocForeignPtrArray, withForeignPtr, plusPtr)
import           Foreign.Ptr (Ptr)

import qualified Orc.Serial.Binary.Internal.Get as Get
import           Orc.Serial.Binary.Internal.OrcNum
import           Orc.Serial.Binary.Internal.Integers.Native

import           System.IO
import           System.IO.Unsafe (unsafePerformIO)

import           Orc.Prelude


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
decodeBase128Varint :: forall w. (OrcNum w) => ByteString -> Either String (Storable.Vector w)
decodeBase128Varint bytes =
  Get.runGet (manyStorable getBase128Varint) bytes


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

        continues =
          testBit byte 7

      if continues then
        go new (7 + shift)
      else
        return (unZigZag new)
  in
    go 0 0

{-# SPECIALIZE getBase128Varint :: Get Int8 #-}
{-# SPECIALIZE getBase128Varint :: Get Int16 #-}
{-# SPECIALIZE getBase128Varint :: Get Int32 #-}
{-# SPECIALIZE getBase128Varint :: Get Int64 #-}
{-# SPECIALIZE getBase128Varint :: Get Int128 #-}
{-# SPECIALIZE getBase128Varint :: Get Word8 #-}
{-# SPECIALIZE getBase128Varint :: Get Word16 #-}
{-# SPECIALIZE getBase128Varint :: Get Word32 #-}
{-# SPECIALIZE getBase128Varint :: Get Word64 #-}
{-# SPECIALIZE getBase128Varint :: Get Word128 #-}

putBase128Varint :: forall w. OrcNum w => Putter w
putBase128Varint =
  let
    go :: Putter (OrcWord w)
    go chunk = do
      let
        masked =
          fromIntegral chunk .&. b01111111

        remainder =
          chunk `shiftR` 7

        terminates =
          remainder == 0

      if terminates then
        Put.putWord8 masked
      else
        Put.putWord8 (masked .|. b10000000) >> go remainder
  in
    go . zigZag
{-# SPECIALIZE putBase128Varint :: Putter Int128 #-}


{-# INLINE decodeIntegerRLEv1 #-}
decodeIntegerRLEv1 :: forall w . OrcNum w => ByteString -> Either String (Storable.Vector w)
decodeIntegerRLEv1 bytes =
  Get.runGet getIntegerRLEv1 bytes


{-# INLINE getIntegerRLEv1 #-}
getIntegerRLEv1 :: forall w . OrcNum w => Get (Storable.Vector w)
getIntegerRLEv1 =
  let
    getSet :: Get (Storable.Vector w)
    getSet = do
      header <- Get.getInt8
      if header >= 0 then do
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


putIntegerRLEv1 :: PutIntegerRLEv1 w => Putter (Storable.Vector w)
putIntegerRLEv1 = Put.putByteString . encodeIntegerRLEv1


{-# INLINE decodeIntegerRLEv2 #-}
decodeIntegerRLEv2 :: forall w . OrcNum w => ByteString ->  Either String (Storable.Vector w)
decodeIntegerRLEv2 =
  Get.runGet getIntegerRLEv2


{-# SPECIALIZE getIntegerRLEv2 :: Get (Storable.Vector Int8) #-}
{-# SPECIALIZE getIntegerRLEv2 :: Get (Storable.Vector Int16) #-}
{-# SPECIALIZE getIntegerRLEv2 :: Get (Storable.Vector Int32) #-}
{-# SPECIALIZE getIntegerRLEv2 :: Get (Storable.Vector Int64) #-}
{-# SPECIALIZE getIntegerRLEv2 :: Get (Storable.Vector Word8) #-}
{-# SPECIALIZE getIntegerRLEv2 :: Get (Storable.Vector Word16) #-}
{-# SPECIALIZE getIntegerRLEv2 :: Get (Storable.Vector Word32) #-}
{-# SPECIALIZE getIntegerRLEv2 :: Get (Storable.Vector Word64) #-}
getIntegerRLEv2 :: forall w . OrcNum w => Get (Storable.Vector w)
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

    -- getLabelledSet :: Get (Storable.Vector w)
    -- getLabelledSet = do
    --   readSoFar <- Get.bytesRead
    --   remaining <- Get.remaining
    --   Get.label ("Read so far: " <> show readSoFar <> "; Remaining: " <> show remaining) getSet

    getSet :: Get (Storable.Vector w)
    getSet = do
      opening  <- Get.lookAhead Get.getWord8
      case opening `shiftR` 6 of
        0 ->
          Get.label "short"
            getShortRepeat

        1 ->
          Get.label "direct"
            getDirect

        2 ->
          Get.label "patched"
            getPatchedBase

        3 ->
          Get.label "delta"
            getDelta

        _ ->
          fail "Impossible!"

  in
    Storable.concat <$>
      consumeMany getSet


{-# INLINE getShortRepeat #-}
getShortRepeat :: forall w . OrcNum w => Get (Storable.Vector w)
getShortRepeat = do
  header <- Get.getWord8
  let
    width =
      (header `shiftR` 3) + 1
    repeats =
      (header .&. 0x07) + 3
  value <-
    Get.getOrcNumBe width
  return $
    Storable.replicate (fromIntegral repeats) value


{-# INLINE getDirect #-}
getDirect :: forall w . OrcNum w => Get (Storable.Vector w)
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

  dataBytes <-
    Get.label ("width: " <> show width) $
    Get.label ("repeats: " <> show repeats) $
      Get.getByteString (ceiling (required % 8))

  return $
    Storable.map (unZigZag . fromIntegral) $
      readLongsNative dataBytes repeats width


{-# INLINE getPatchedBase #-}
getPatchedBase :: forall w . OrcNum w => Get (Storable.Vector w)
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
    Get.getOrcNumBePatchedBase baseWidth

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
getDelta :: forall w . OrcNum w => Get (Storable.Vector w)
getDelta = do
  header <- Get.getWord16be
  let
    headerWidth =
      fromIntegral $
        (header .&. 0x3E00) `shiftR` 9

    width =
      if headerWidth == 0 then
        0
      else
        bitSizeLookup headerWidth

    repeats =
      fromIntegral $
        (header .&. 0x01FF) + 1

    deltaRepeats =
      if repeats == 1 then
        0
      else
        repeats - 2

    required =
      deltaRepeats * width

  baseValue <-
    getBase128Varint

  (deltaBase, deltaSgn) <-
    (((fromIntegral . abs) &&& signum) . unZigZag64) <$>
      getBase128Varint

  deltaBytes <-
    Get.getByteString (ceiling (required % 8))

  let
    deltas =
      Storable.map fromIntegral $
        readLongsNative deltaBytes deltaRepeats width

    op =
      if deltaSgn < 0 then (-) else (+)

    scanVec =
      if repeats == 1 then
        Storable.empty
      else if width == 0 then
        Storable.replicate (fromIntegral deltaRepeats + 1) deltaBase
      else
        Storable.singleton deltaBase <> deltas

  return $
    Storable.scanl' op baseValue scanVec


readLongsNative :: ByteString -> Word64 -> Word64 -> Storable.Vector Word64
readLongsNative bytes len bitsize =
  unsafePerformIO $ do
    let
      (inPtr, offset, _inLen) =
        ByteString.toForeignPtr bytes

    outPtr <-
      mallocForeignPtrArray $
        fromIntegral len

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


-- | Fixup a nanoseconds.
--
--   The specification appears to be incorrect in specifying how to
--   parse nanoseconds, or rather, it doesn't try. There's a broken
--   sentence fragment, but whoever was writing it forgot to finish.
decodeNanoseconds :: Word64 -> Word64
decodeNanoseconds nano =
  let
    zeros =
      nano .&. 0x07
    result =
      nano `shiftR` 3
  in
    if zeros == 0 then
      result
    else
      result * (10 ^ (zeros + 1))
{-# INLINE decodeNanoseconds #-}


encodeNanoseconds :: Word64 -> Word64
encodeNanoseconds 0 = 0
encodeNanoseconds nano =
  let
    (nano_, zeros_) =
      normalizePositive (nano, 0)
  in
    if (zeros_ > 1) then
      (nano_ `shiftL` 3) .|. (zeros_ - 1)
    else
      nano `shiftL` 3
{-# INLINE encodeNanoseconds #-}


normalizePositive :: (Word64, Word64) -> (Word64, Word64)
normalizePositive (0, n) = (0, n)
normalizePositive (!c, !n) =
  case divMod c 10 of
    (c', r)
      | r  == 0   -> normalizePositive (c', n + 1)
      | otherwise -> (c, n)
