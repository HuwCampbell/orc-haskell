
{-# LANGUAGE NoImplicitPrelude   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE PatternGuards #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}

-- | A Monad Transformer for throwing OrcExceptions
--
--   One doesn't have to use this class if they don't want to,
--   instead it is provided as a convenience if one would prefer
--   to consume the Orc file in IO.
module Orc.Exception.Raising (
    OrcException (..)
  , Raising (..)
) where

import           Control.Monad.IO.Class
import           Control.Monad.Primitive (PrimMonad (..))

import           Control.Exception
import           Control.Monad.Base (MonadBase (..))
import           Control.Monad.Except (MonadError (..), MonadTrans (..))
import           Control.Monad.Trans.Control (MonadBaseControl (..), MonadTransControl (..), control, defaultLiftBaseWith, defaultRestoreM)

import           Orc.Exception.Type
import           Orc.Prelude

-- | A newtype over IO which acts as the identity transformer.
--
--   This is used to provide MonadTransControl and an instance
--   for throwing OrcExceptions.
--
--   We provide two ways of handling badly formed Orc files,
--   this transformer will throw OrcExceptions in IO, while
--   `EitherT OrcException m a` can be used to handle them
--   explicitly in an either.
newtype Raising io a =
  Raising {
    raisingOrcErrors :: io a
  } deriving (Functor, Applicative, Monad, MonadIO)

instance MonadTrans Raising where
  lift = Raising
  {-# INLINABLE lift #-}

instance MonadTransControl Raising where
  type StT Raising a = a
  liftWith f = Raising $ f $ raisingOrcErrors
  restoreT = Raising
  {-# INLINABLE liftWith #-}
  {-# INLINABLE restoreT #-}

instance PrimMonad io => PrimMonad (Raising io) where
  type PrimState (Raising io) = PrimState io
  primitive = Raising . primitive
  {-# INLINE primitive #-}

instance MonadBase IO io => MonadBase IO (Raising io) where
  liftBase =
    lift . liftBase
  {-# INLINE liftBase #-}

instance MonadBaseControl IO io => MonadBaseControl IO (Raising io) where
  type StM (Raising io) a = StM io a
  liftBaseWith = defaultLiftBaseWith
  {-# INLINE liftBaseWith #-}
  restoreM = defaultRestoreM
  {-# INLINE restoreM #-}

instance MonadBaseControl IO io => MonadError OrcException (Raising io) where
  throwError =
    liftBase . throwIO
  {-# INLINE throwError #-}

  catchError a handler =
    control $ \runInIO ->
      catch (runInIO a)
        (\e -> runInIO $ handler e)
  {-# INLINE catchError #-}
