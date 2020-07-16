{-# LANGUAGE CPP               #-}
{-# LANGUAGE DeriveFoldable    #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}
module Orc.Prelude (
  -- * Primitive types
  -- ** Bool
    Bool (..)
  , bool
  , (&&)
  , (||)
  , not
  , otherwise
  -- ** Char
  , Char
  -- ** Int
  , Integer
  , Int
  , Int8
  , Int16
  , Int32
  , Int64
  , Double
  , Float
  -- ** Word
  , Word64
  -- ** Real
  , fromIntegral
  , ceiling
  -- * Text
  , Text
  -- * Ratio
  , Ratio
  , (%)
  -- * Algebraic structures
  -- ** Monoid
  , Monoid (..)
  , (<>)
  -- ** Functor
  , Functor (..)
  , (<$>)
  , ($>)
  , void
  , with
  -- ** Bifunctor
  , Bifunctor (..)
  -- ** Applicative
  , Applicative (..)
  , (<**>)
  -- ** Alternative
  , Alternative (..)
  , asum
  -- ** Monad
  , Monad ((>>=), (>>), return)
  , join
  , bind
  , (=<<)
  , unless
  , when

    -- ** MonadFail
  , Fail.MonadFail (..)

  -- ** MonadPlus
  , MonadPlus (..)
  , guard
  , msum

  -- * Data structures
  -- ** Either
  , Either (..)
  , either
  , note
  -- ** Maybe
  , Maybe (..)
  , fromMaybe
  , maybe
  , hush
  -- ** Tuple
  , fst
  , snd
  , curry
  , uncurry

  -- * Typeclasses
  -- ** Enum
  , Enum (..)
  -- ** Num
  , Num (..)
  , Fractional (..)
  , Integral (..)

  -- ** Eq
  , Eq (..)
  -- ** Read
  , Read (..)
  , readEither
  , readMaybe
  -- ** Show
  , Show (..)
  -- *** ShowS
  , ShowS
  , showString
  -- ** Foldable
  , Foldable (..)
  , for_
  , all
  -- ** Ord
  , Ord (..)
  , Ordering (..)
  , comparing
  -- ** Traversable
  , Traversable (..)
  , for
  , traverse_

  -- * Strict Maybe
  ,  Maybe'(..)
  , fromMaybe'
  , fromMaybeM'
  , isJust'
  , isNothing'
  , maybe'

  -- * Combinators
  , id
  , (.)
  , ($)
  , ($!)
  , (&)
  , (^)
  , (^^)
  , const
  , flip
  , fix
  , on
  , seq

  -- * System
  -- ** IO
  , IO
  , FilePath

  -- * Partial functions
  , undefined
  , error

  -- * Debugging facilities
  , trace
  , traceM
  , traceIO
  ) where


import           Control.Monad as Monad (
           Monad (..)
         , MonadPlus (..)
         , guard
         , join
         , msum
         , (=<<)
         , unless
         , when
         )

import qualified Control.Monad.Fail as Fail

import           Control.Applicative as Applicative (
           Applicative (..)
         , (<**>)
         , Alternative (..)
         , empty
         )

import           Data.Bifunctor as Bifunctor (
           Bifunctor (..)
         )
import           Data.Bool as Bool (
           Bool (..)
         , bool
         , (&&)
         , (||)
         , not
         , otherwise
         )
import           Data.Char as Char (
           Char
         )
import           Data.Either as Either (
           Either (..)
         , either
         )
import           Data.Foldable as Foldable (
           Foldable (..)
         , asum
         , traverse_
         , for_
         , all
         )
import           Data.Function as Function (
           id
         , (.)
         , ($)
         , (&)
         , const
         , flip
         , fix
         , on
         )
import           Data.Functor as Functor (
           Functor (..)
         , (<$>)
         , ($>)
         , void
         )
import           Data.Eq as Eq (
           Eq (..)
         )
import           Data.Int as Int (
           Int
         , Int8
         , Int16
         , Int32
         , Int64
         )
import           Data.Maybe as Maybe (
           Maybe (..)
         , fromMaybe
         , maybe
         )
import           Data.Monoid as Monoid (
           Monoid (..)
         , (<>)
         )
import           Data.Ord as Ord (
           Ord (..)
         , Ordering (..)
         , comparing
         )

import           Data.Text as Text (
           Text
         )
import           Data.Ratio as Ratio (
           Ratio
         , (%)
         )

import           Data.Traversable as Traversable (
           Traversable (..)
         , for
         )
import           Data.Tuple as Tuple (
           fst
         , snd
         , curry
         , uncurry
         )
import           Data.Word as Word (
           Word64
         )

import qualified Debug.Trace as Trace

import           GHC.Real as Real (
           fromIntegral
         , fromRational
         )
#if MIN_VERSION_base(4,9,0)
import           GHC.Stack (HasCallStack)
#endif

import           Prelude as Prelude (
           Enum (..)
         , Num (..)
         , Fractional (..)
         , Integral (..)
         , Integer
         , Double
         , Float
         , seq
         , ($!)
         , ceiling
         , (^)
         , (^^)
         )
import qualified Prelude as Unsafe

import           System.IO as IO (
           FilePath
         , IO
         )

import           Text.Read as Read (
           Read (..)
         , readEither
         , readMaybe
         )
import           Text.Show as Show (
           Show (..)
         , ShowS
         , showString
         )


#if MIN_VERSION_base(4,9,0)
undefined :: HasCallStack => a
#else
undefined :: a
#endif
undefined =
  Unsafe.undefined
{-# WARNING undefined "'undefined' is unsafe" #-}

#if MIN_VERSION_base(4,9,0)
error :: HasCallStack => [Char] -> a
#else
error :: [Char] -> a
#endif
error =
  Unsafe.error
{-# WARNING error "'error' is unsafe" #-}

trace :: [Char] -> a -> a
trace =
  Trace.trace
{-# WARNING trace "'trace' should only be used while debugging" #-}

#if MIN_VERSION_base(4,9,0)
traceM :: Applicative f => [Char] -> f ()
#else
traceM :: Monad m => [Char] -> m ()
#endif
traceM =
  Trace.traceM
{-# WARNING traceM "'traceM' should only be used while debugging" #-}

traceIO :: [Char] -> IO ()
traceIO =
  Trace.traceIO
{-# WARNING traceIO "'traceIO' should only be used while debugging" #-}

bind :: Monad m => (a -> m b) -> m a -> m b
bind = flip (>>=)

with :: Functor f => f a -> (a -> b) -> f b
with =
  flip fmap
{-# INLINE with #-}

-- | Tag a 'Nothing'.
note :: a -> Maybe b -> Either a b
note a Nothing = Left a
note _ (Just b) = Right b
{-# INLINEABLE note #-}

-- | Eliminate a 'Left'.
hush :: Either a b -> Maybe b
hush (Left _) = Nothing
hush (Right b) = Just b
{-# INLINEABLE hush #-}



-- | Strict version of 'Data.Maybe.Maybe'.
data Maybe' a =
    Just' !a
  | Nothing'
  deriving (Eq, Ord, Read, Show, Foldable, Traversable)

instance Functor Maybe' where
  fmap _ Nothing' = Nothing'
  fmap f (Just' x) = Just' (f x)

instance Applicative Maybe' where
  pure = Just'

  Just' f <*> m = fmap f m
  Nothing' <*> _m = Nothing'

  Just' _m1 *> m2 = m2
  Nothing' *> _m2 = Nothing'

instance Alternative Maybe' where
  empty =
    Nothing'

  (<|>) l r =
    case l of
      Nothing' ->
        r
      Just' _ ->
        l

instance MonadPlus Maybe' where
  mzero =
    empty

  mplus =
    (<|>)

-- | Not technically a monad due to bottom, but included anyway as we don't
-- use partial functions.
instance Monad Maybe' where
  (Just' x) >>= k = k x
  Nothing' >>= _ = Nothing'

  (>>) = (*>)

  return = Just'

#if !(MIN_VERSION_base(4,13,0))
  fail = Fail.fail
#endif

instance Fail.MonadFail Maybe' where
  fail _ = Nothing'


maybe' :: b -> (a -> b) -> Maybe' a -> b
maybe' n _ Nothing' = n
maybe' _ f (Just' x) = f x

isJust' :: Maybe' a -> Bool
isJust' Nothing' = False
isJust' (Just' _) = True

isNothing' :: Maybe' a -> Bool
isNothing' Nothing' = True
isNothing' (Just' _) = False

fromMaybe' :: a -> Maybe' a -> a
fromMaybe' x Nothing' = x
fromMaybe' _ (Just' y) = y

fromMaybeM' :: Applicative f => f a -> Maybe' a -> f a
fromMaybeM' = flip maybe' pure