module Main (
  main
) where

import           Control.Monad.Trans.Either.Exit
import           Data.Text (pack)

import           Orc.Serial.Binary.Striped
import           Orc.Serial.Binary.Logical

import           Options.Applicative

import qualified Streaming.Prelude as Streaming

parser :: Parser Command
parser =
  hsubparser $ mconcat [
    command "json"     (info json    (progDesc "Print and ORC file as JSON rows"))
  , command "reencode" (info rewrite (progDesc "Re-write an Orc file with this encoder"))
  ]


data Command
  = Json FilePath
  | Rewrite FilePath FilePath
  deriving (Eq, Show)


json :: Parser Command
json = Json <$> strArgument (metavar "ORC_FILE" <> help "Orc file to print as JSON")


rewrite :: Parser Command
rewrite = Rewrite <$> strArgument (metavar "ORC_FILE") <*> strArgument (metavar "OUT_ORC_FILE")


main :: IO ()
main = do
  cmnd <- customExecParser (prefs showHelpOnEmpty) (info (parser <**> helper) idm)

  orDie pack $
    case cmnd of
      Json orcIn ->
        printOrcFile orcIn

      Rewrite orcIn orcOut ->
        withOrcStripes orcIn $
          putOrcFile orcOut . Streaming.map snd

