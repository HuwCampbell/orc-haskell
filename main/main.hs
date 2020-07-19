module Main (
  main
) where

import Data.Text (pack)
import Orc.Serial.Binary.Striped
import Orc.Serial.Binary.Logical
import Options.Applicative
import Control.Monad.Trans.Either.Exit

import qualified Streaming.Prelude as Streaming

parser :: Parser (FilePath, Maybe FilePath)
parser = (,) <$> strArgument (metavar "ORC_FILE") <*> optional (strArgument (metavar "OUT_ORC_FILE"))

main :: IO ()
main = do
  (fp, oFop) <- execParser (info (parser <**> helper) idm)

  orDie pack $
    case oFop of
      Nothing ->
        printOrcFile fp

      Just fop ->
        withOrcStripes fp $
          putOrcFile fop . Streaming.map snd

