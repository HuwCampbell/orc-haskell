{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE PatternGuards #-}

module Orc.Serial.Binary.Internal.Bytes (
    decodeBytes
  , decodeBits

  , encodeBytes
  , encodeBits

  , putBytes
  , putBytesNative
  , putBits

  -- For Benchmarking
  , decodeBytesNative
  , encodeBytesNative
) where

import           Data.Bits (testBit, setBit)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as ByteString
import           Data.Word (Word8)

import           Data.Serialize.Get (Get)
import qualified Data.Serialize.Get as Get
import           Data.Serialize.Put (Putter)
import qualified Data.Serialize.Put as Put

import           Data.String (String)

import qualified Data.Vector.Storable as Storable

import           Foreign (withForeignPtr)
import           Foreign.Ptr (Ptr, plusPtr)

import           System.IO.Unsafe (unsafePerformIO)
import           System.IO as IO

import           Orc.Prelude


{-# INLINE decodeBytesNative #-}
decodeBytesNative :: Word64 -> ByteString -> Storable.Vector Word8
decodeBytesNative len bytes =
  unsafePerformIO $ do
    let
      (inPtr, offset, _inLen) =
        ByteString.toForeignPtr bytes

    outPtr <-
      ByteString.mallocByteString (fromIntegral len)

    withForeignPtr inPtr $ \inPtr' ->
      withForeignPtr outPtr $ \outPtr' ->
        read_bytes_rle (fromIntegral len) (inPtr' `plusPtr` offset) outPtr'

    return $
      Storable.unsafeFromForeignPtr outPtr 0 (fromIntegral len)


{-# INLINE putBytesNative #-}
putBytesNative :: Putter (Storable.Vector Word8)
putBytesNative = Put.putByteString . encodeBytesNative

{-# INLINE encodeBytesNative #-}
encodeBytesNative :: Storable.Vector Word8 -> ByteString
encodeBytesNative bytes =
  unsafePerformIO $ do
    let
      (inPtr, offset, _inLen) =
        Storable.unsafeToForeignPtr bytes

      len = Storable.length bytes

    outPtr <-
      ByteString.mallocByteString (len + len `div` 128)

    reLen <- withForeignPtr inPtr $ \inPtr' ->
      withForeignPtr outPtr $ \outPtr' ->
        write_bytes_rle (fromIntegral len) (inPtr' `plusPtr` offset) outPtr'

    return $
      ByteString.fromForeignPtr outPtr 0 (fromIntegral reLen)


foreign import ccall unsafe
  read_bytes_rle
    :: Int64 -> Ptr Word8 -> Ptr Word8 -> IO ()

foreign import ccall unsafe
  write_bytes_rle
    :: Int64 -> Ptr Word8 -> Ptr Word8 -> IO Int64


{-# INLINE decodeBytes #-}
decodeBytes :: ByteString -> Either String (Storable.Vector Word8)
decodeBytes =
    Get.runGet getBytes

{-# INLINE encodeBytes #-}
encodeBytes :: (Storable.Vector Word8) -> ByteString
encodeBytes =
    Put.runPut . putBytes


{-# INLINE getBytes #-}
getBytes :: Get (Storable.Vector Word8)
getBytes =
  let
    getSet :: Get (Storable.Vector Word8)
    getSet = do
      header <- Get.getInt8
      if header >= 0
        then do
          let
            runLength = fromIntegral header + 3;
          Storable.replicate runLength <$> Get.getWord8
        else do
          let
            listLength = negate $ fromIntegral header
          Storable.replicateM listLength Get.getWord8

  in
    Storable.concat <$>
      many getSet


putBytes :: Putter (Storable.Vector Word8)
putBytes =
  let
    toRuns :: Storable.Vector Word8 -> [(Word8, Word8)]
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


    takeLiterals :: [(Word8, Word8)] -> ([(Word8, Word8)], [(Word8, Word8)])
    takeLiterals =
      let
        go :: Word8 -> [(Word8, Word8)] -> ([(Word8, Word8)], [(Word8, Word8)])
        go n rest
          | (x, i) : xs <- rest
          , i < 3
          , n + i < 128
          = let (r, rs) = go (n + i) xs
            in  ((x,i):r, rs)
          | otherwise
          = ([], rest)

      in go 0

    putSet :: Putter (Storable.Vector Word8)
    putSet words =
      let
        runs = toRuns words

        place []
          = pure ()
        place ws@((w, n):ws0)
          | n >= 3
          = do Put.putWord8 (n - 3)
               Put.putWord8 w
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
                  for_ noRuns $ \(ww,i) ->
                    for_ (enumFromTo 1 i) $ \_ ->
                      Put.putWord8 ww

                  place runStart

      in
        place runs
  in
    putSet



{-# INLINABLE decodeBits #-}
decodeBits :: ByteString -> Either String (Storable.Vector Bool)
decodeBits =
  let
    finiteBits w =
      Storable.map (testBit w) (Storable.fromList [7,6,5,4,3,2,1,0])

  in \bytes -> do
    decodedByteString <-
      decodeBytes bytes

    let
      allBytesWorth =
        Storable.concatMap finiteBits decodedByteString

    pure $
      allBytesWorth


{-# INLINABLE putBits #-}
putBits :: Putter (Storable.Vector Bool)
putBits =
    putBytes . Storable.unfoldr go
  where
    go bools
      | Storable.null bools = Nothing

    go bools = do
      let
        (b1, rest) =
          Storable.splitAt 8 bools

        toWrite =
          Storable.ifoldl' sets 0 b1

      Just(toWrite, rest)

    sets acc i b =
      if b then
        setBit acc (7 - i)
      else
        acc

{-# INLINE encodeBits #-}
encodeBits :: (Storable.Vector Bool) -> ByteString
encodeBits =
    Put.runPut . putBits

