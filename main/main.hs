module Main (
  main
) where

import Data.Text (pack)
import Orc.Serial.Seek
import Options.Applicative
import Control.Monad.Trans.Either.Exit

parser :: Parser (FilePath, Bool)
parser = (,) <$> strArgument (metavar "ORC_FILE") <*> switch (long "schema")

main :: IO ()
main = do
  (fp, _) <- execParser (info (parser <**> helper) idm)
  orDie pack (printOrcFile fp)
