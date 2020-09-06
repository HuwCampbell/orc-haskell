{-# LANGUAGE DoAndIfThenElse #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Orc.Serial.Binary.Internal.Compression.Snappy (
    compress
  , decompress
  ) where

import qualified Data.ByteString as B
import           Data.ByteString.Internal (ByteString(..), createUptoN, mallocByteString)
import           Data.Word (Word8)

import           Foreign.C.Types (CSize(..), CInt(..))
import           Foreign.ForeignPtr (withForeignPtr)
import           Foreign.Marshal (alloca)
import qualified Foreign.Marshal as Foreign
import           Foreign.Ptr (Ptr, plusPtr)
import           Foreign.Storable (peek)

import           Orc.Prelude

import           System.IO.Unsafe (unsafePerformIO)


--
-- size_t snappy_max_compressed_length(size_t source_length)
--
foreign import ccall unsafe "snappy_max_compressed_length"
  snappy_max_compressed_length :: CSize -> IO CSize

--
-- snappy_status snappy_uncompressed_length(const char* compressed,
--                                          size_t compressed_length,
--                                          size_t* result)
--
foreign import ccall unsafe "snappy_uncompressed_length"
  snappy_uncompressed_length :: Ptr Word8 -> CSize -> Ptr CSize -> IO CInt

--
-- void snappy_raw_compress(const char* input,
--                          size_t input_length,
--                          char* compressed,
--                          size_t *compressed_length)
--
foreign import ccall unsafe "snappy_raw_compress"
  snappy_raw_compress :: Ptr Word8 -> CSize -> Ptr Word8 -> Ptr CSize -> IO ()

--
-- snappy_status snappy_raw_uncompress(const char* compressed,
--                                     size_t compressed_length,
--                                     char* uncompressed)
--
foreign import ccall unsafe "snappy_raw_uncompress"
  snappy_raw_uncompress :: Ptr Word8 -> CSize -> Ptr Word8 -> IO CInt


--
-- Note that we use snappy_raw_compress and snappy_raw_uncompress instead of
-- the standard Snappy C API because it avoids additional checks on the lengths
-- of buffers which we do in the Haskell instead. The main issue with the extra
-- checks is that we then have to deal with the C API signaling a failure which
-- cannot happen, and this would mean both compress and decompress would have
-- the unpleasant type: ByteString -> Either SnappyError ByteString.
--


-- | Compress a 'ByteString' using snappy.
compress :: ByteString -> ByteString
compress (PS sfp soff slen0) =
  unsafePerformIO $ do
    let
      slen = fromIntegral slen0
    dlen0 <- snappy_max_compressed_length slen
    createUptoN (fromIntegral dlen0) $ \dptr ->
      withForeignPtr sfp $ \sptr0 ->
      Foreign.with dlen0 $ \dlenptr -> do
        snappy_raw_compress (sptr0 `plusPtr` soff) slen dptr dlenptr
        dlen <- peek dlenptr
        return $ fromIntegral dlen

-- | Decompress a 'ByteString' using snappy.
decompress :: ByteString -> Maybe ByteString
decompress (PS sfp soff slen0) =
  unsafePerformIO $ do
    withForeignPtr sfp $ \sptr0 -> do
      let
        sptr = sptr0 `plusPtr` soff
        slen = fromIntegral slen0
      alloca $ \dlenptr -> do
        r0 <- snappy_uncompressed_length sptr slen dlenptr
        if r0 /= 0 then
          return Nothing
        else do
          dlen <- fromIntegral <$> peek dlenptr
          case dlen of
            0 ->
              return $ Just B.empty
            _ -> do
              dfp <- mallocByteString dlen
              withForeignPtr dfp $ \dptr -> do
                r1 <- snappy_raw_uncompress sptr slen dptr
                if r1 /= 0 then
                  return Nothing
                else
                  return . Just $ PS dfp 0 dlen
