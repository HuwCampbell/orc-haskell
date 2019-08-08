{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# LANGUAGE OverlappingInstances   #-}


module Yolo where

import System.IO
import System.IO.Unsafe

import Text.Show.Pretty

import Control.Monad.Trans.Except
import Control.Monad.Trans.Resource




yoloPrint :: (Show a, Yolo f) => f a -> IO ()
yoloPrint = putStr . ppShow . yolo


class Yolo f where
  yolo :: f a -> a

instance Yolo Maybe where
  yolo (Just x) = x
  yolo Nothing = error "Yolo!... You got nothing"

instance Yolo (Either a) where
  yolo (Right x) = x

instance Yolo (Either String) where
  yolo (Right x) = x
  yolo (Left x) = error x

instance Yolo [] where
  yolo = head

instance Yolo IO where
  yolo = unsafePerformIO

instance Yolo m => Yolo (ExceptT e m) where
  yolo = yolo . yolo . runExceptT

instance Yolo m => Yolo (ExceptT String m) where
  yolo = yolo . yolo . runExceptT

instance (Yolo m, MonadBaseControl IO m) => Yolo (ResourceT m) where
  yolo = yolo . runResourceT
