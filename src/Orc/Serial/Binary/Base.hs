{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ConstraintKinds     #-}

module Orc.Serial.Binary.Base (
    withOrcFile
  , checkOrcFile

  -- * Internal
  , withBinaryFileLifted
  , checkMagic

  , MonadTransIO
) where

import           Control.Monad.IO.Class
import           Control.Monad.Primitive (PrimMonad)

import           Control.Monad.Except (MonadError, liftEither)
import           Control.Monad.Trans.Control (MonadTransControl (..))

import qualified Data.Serialize.Get as Get

import           Data.String (String)
import qualified Data.ByteString as ByteString

import           Orc.Schema.Types as Orc

import           Orc.Serial.Protobuf.Schema as Orc
import           Orc.Serial.Binary.Internal.Compression

import           System.IO as IO

import           Orc.Prelude


type MonadTransIO t = (MonadError String (t IO), MonadIO (t IO), PrimMonad (t IO), MonadTransControl t)


withBinaryFileLifted
  :: (MonadIO (t IO), MonadTransControl t)
  => FilePath
  -> IOMode
  -> (Handle -> t IO r)
  -> t IO r
withBinaryFileLifted file mode action =
  liftWith (\run -> withBinaryFile file mode (run . action)) >>=
    restoreT . return


withOrcFile
  :: MonadTransIO t
  => FilePath -> ((Handle, PostScript, Footer) -> t IO r) -> t IO r
withOrcFile file action =
  withBinaryFileLifted file ReadMode $ \handle -> do
    (postScript, footer) <-
      checkMagic handle

    action (handle, postScript, footer)


checkOrcFile
  :: MonadTransIO t
  => FilePath -> t IO ([StripeInformation], Type, Maybe CompressionKind)
checkOrcFile file =
  withOrcFile file $ \(_, postScript, footer) -> do
    let
      stripeInfos =
        stripes footer

      typeInfo =
        types footer

    return (stripeInfos, typeInfo, compression postScript)



-- | Checks that the magic values are present in the file
--   (i.e. makes sure that it is actually an ORC file)
checkMagic :: MonadTransIO t => Handle -> t IO (PostScript, Footer)
checkMagic handle = do
  liftIO  $ hSeek handle AbsoluteSeek 0
  header <- liftIO (ByteString.hGet handle 3)

  unless (header == "ORC") $
    liftEither (Left "Invalid header - probably not an ORC file.")

  -- Seek to the last byte of the file to get the
  -- size of the postscript.
  liftIO  $
    hSeek handle SeekFromEnd (-1)

  psLength <-
    liftEither . Get.runGet Get.getWord8 =<< liftIO (ByteString.hGet handle 1)

  liftIO $
    hSeek handle SeekFromEnd (negate $ 1 + fromIntegral psLength)

  postScript <-
    liftEither . readPostScript =<< liftIO (ByteString.hGet handle $ fromIntegral psLength)

  let
    compressionInfo =
      compression postScript

    footerLen =
      footerLength postScript

  liftIO $
    hSeek handle SeekFromEnd (negate $ 1 + fromIntegral psLength + fromIntegral footerLen)

  footerData <-
    liftEither . readCompressedStream compressionInfo =<< liftIO (ByteString.hGet handle $ fromIntegral footerLen)

  footer <-
    liftEither $ readFooter footerData

  liftIO $
    hSeek handle AbsoluteSeek 3

  return (postScript, footer)
