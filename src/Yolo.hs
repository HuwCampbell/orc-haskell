{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE UndecidableInstances   #-}
module Yolo where

import GHC.Stack
import System.IO.Unsafe

import qualified Viking

import Text.Show.Pretty

import Control.Monad.Identity (Identity (..))
import Control.Monad.Trans.Except
import Control.Monad.Trans.Identity
import Control.Monad.Trans.Resource
import Control.Monad.Trans.Control (MonadTransControl (..), MonadBaseControl (..), defaultRestoreM)

yoloPrint :: (Show a, Yolo f) => f a -> IO ()
yoloPrint = putStr . ppShow . yolo

class Yolo f where
  yolo :: HasCallStack => f a -> a

instance Yolo Identity where
  yolo (Identity x) = x

instance Yolo Maybe where
  yolo (Just x) = x
  yolo Nothing = error "Yolo!... Nothing"

instance Yolo (Either a) where
  yolo (Right x) = x
  yolo (Left _) = error "Yolo!... Left"

instance {-# OVERLAPPING #-} Yolo (Either String) where
  yolo (Right x) = x
  yolo (Left x) = error x

instance Yolo [] where
  yolo = head

instance Yolo IO where
  yolo = unsafePerformIO

instance Yolo m => Yolo (ExceptT e m) where
  yolo = yolo . yolo . runExceptT

instance Yolo m => Yolo (IdentityT m) where
  yolo = yolo . runIdentityT

instance {-# OVERLAPPING #-} Yolo m => Yolo (ExceptT String m) where
  yolo = yolo . yolo . runExceptT

instance (Yolo m, MonadUnliftIO m) => Yolo (ResourceT m) where
  yolo = yolo . runResourceT
