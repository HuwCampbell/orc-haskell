module Main (
  main
) where

import Orc.Serial.Seek
import Options.Applicative

parser :: Parser (FilePath, Bool)
parser = (,) <$> strArgument (metavar "ORC_FILE") <*> switch (long "schema")

main :: IO ()
main = do
  (fp, _) <- execParser (info (parser <**> helper) idm)
  printOrcFile fp



