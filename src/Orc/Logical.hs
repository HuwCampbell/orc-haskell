{- | Streaming of ORC files as 'Logical.Row'.

     This module provides two separate mechanisms for dealing with
     invalid Orc files.

     To stream entirely in IO, one can just use @withOrcFile@ and
     @putOrcFile@
-}
module Orc.Logical (
    -- * IO interface
    Logical.withOrcFile
  , Logical.putOrcFile

    -- * Lifted interface
  , Logical.withOrcFileLifted
  , Logical.putOrcFileLifted

    -- * Convenience functions
  , Logical.printOrcFile

    -- * Reëxports
  , module Orc.Table.Logical

    -- * Types
  , Types.Type
  , Types.CompressionKind
) where

import qualified Orc.Serial.Binary.Logical as Logical
import           Orc.Table.Logical
import qualified Orc.Schema.Types as Types
