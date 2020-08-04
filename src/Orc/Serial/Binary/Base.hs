{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ConstraintKinds     #-}

module Orc.Serial.Binary.Base (
    withOrcFileLifted
  , checkOrcFile

  -- * Internal
  , withBinaryFileLifted
  , checkMagic

  , MonadTransIO
  , OrcException (..)
  , Raising (..)
  , liftEitherString
) where

import           Control.Monad.IO.Class
import           Control.Monad.Primitive (PrimMonad)

import           Control.Monad.Except (MonadError, liftEither)
import           Control.Monad.Trans.Control (MonadTransControl (..))
import           Control.Monad.Trans.Either (EitherT)

import qualified Data.Serialize.Get as Get

import           Data.String (String)
import qualified Data.ByteString as ByteString

import           Orc.Exception.Raising

import           Orc.Schema.Types as Orc

import           Orc.Serial.Protobuf.Schema as Orc
import           Orc.Serial.Binary.Internal.Compression

import           System.IO as IO

import           Orc.Prelude


-- | Top type synonym for reading and writing ORC files.
--
--   Essentially, we can use any Monad transformer over IO, but, we need to have some way of handing errors
--   in the ORC file.
--
--   There are essentially only two instances which make sense, an `EitherT OrcException m`, and the custom
--   `@Raising@ m` type from "Orc.Exception.Raising".
type MonadTransIO t = (MonadError OrcException (t IO), MonadIO (t IO), PrimMonad (t IO), MonadTransControl t)


liftEitherWith :: MonadError e m => (b -> e) -> Either b a -> m a
liftEitherWith f =
  liftEither . first f


liftEitherString ::  MonadError OrcException m => Either String a -> m a
liftEitherString =
  liftEitherWith OrcException


withBinaryFileLifted
  :: (MonadIO (t IO), MonadTransControl t)
  => FilePath
  -> IOMode
  -> (Handle -> t IO r)
  -> t IO r
withBinaryFileLifted file mode action =
  liftWith (\run -> withBinaryFile file mode (run . action)) >>=
    restoreT . return


withOrcFileLifted
  :: MonadTransIO t
  => FilePath -> ((Handle, PostScript, Footer) -> t IO r) -> t IO r
withOrcFileLifted file action =
  withBinaryFileLifted file ReadMode $ \handle -> do
    (postScript, footer) <-
      checkMagic handle

    action (handle, postScript, footer)

{-# SPECIALIZE
  withOrcFileLifted
    :: FilePath
    -> ((Handle, PostScript, Footer) -> EitherT OrcException IO r)
    -> EitherT OrcException IO r #-}

{-# SPECIALIZE
  withOrcFileLifted
    :: FilePath
    -> ((Handle, PostScript, Footer) -> Raising IO r)
    -> Raising IO r #-}

checkOrcFile
  :: MonadTransIO t
  => FilePath -> t IO ([StripeInformation], Type, Maybe CompressionKind)
checkOrcFile file =
  withOrcFileLifted file $ \(_, postScript, footer) -> do
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
    liftEitherString (Left "Invalid header - probably not an ORC file.")

  -- Seek to the last byte of the file to get the
  -- size of the postscript.
  liftIO  $
    hSeek handle SeekFromEnd (-1)

  psLength <-
    liftEitherString . Get.runGet Get.getWord8 =<< liftIO (ByteString.hGet handle 1)

  liftIO $
    hSeek handle SeekFromEnd (negate $ 1 + fromIntegral psLength)

  postScript <-
    liftEitherString . readPostScript =<< liftIO (ByteString.hGet handle $ fromIntegral psLength)

  let
    compressionInfo =
      compression postScript

    footerLen =
      footerLength postScript

  liftIO $
    hSeek handle SeekFromEnd (negate $ 1 + fromIntegral psLength + fromIntegral footerLen)

  footerData <-
    liftEitherString . readCompressedStream compressionInfo =<< liftIO (ByteString.hGet handle $ fromIntegral footerLen)

  footer <-
   liftEitherString $ readFooter footerData

  liftIO $
    hSeek handle AbsoluteSeek 3

  return (postScript, footer)
