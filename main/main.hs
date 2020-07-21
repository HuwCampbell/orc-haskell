module Main (
  main
) where

import           Control.Monad.Trans.Either.Exit
import           Data.Text (pack)

import           Orc.Serial.Binary.Striped
import           Orc.Serial.Binary.Logical
import           Orc.Table.Convert
import           Orc.Schema.Types

import           Options.Applicative

import qualified Streaming.Prelude as Streaming

parser :: Parser Command
parser =
  hsubparser $ mconcat [
    command "json"      (info json      (progDesc "Print and ORC file as JSON rows"))
  , command "reencode"  (info rewrite   (progDesc "Re-write an Orc file with this encoder"))
  , command "roundtrip" (info roundTrip (progDesc "Same as reencode, but going via logical representation"))
  ]


data Command
  = Json FilePath
  | Rewrite FilePath FilePath (Maybe CompressionKind)
  | RoundTrip FilePath FilePath (Maybe CompressionKind) Int
  deriving (Eq, Show)


json :: Parser Command
json = Json <$> strArgument (metavar "ORC_FILE" <> help "Orc file to print as JSON")


rewrite :: Parser Command
rewrite =
  Rewrite
    <$> strArgument (metavar "ORC_FILE")
    <*> strArgument (metavar "OUT_ORC_FILE")
    <*> optional (option cmprssnReadM (long "compression"))

roundTrip :: Parser Command
roundTrip =
  RoundTrip
    <$> strArgument (metavar "ORC_FILE")
    <*> strArgument (metavar "OUT_ORC_FILE")
    <*> optional (option cmprssnReadM (long "compression"))
    <*> option auto (long "chunk-size" <> Options.Applicative.value 10000)

cmprssnReadM :: ReadM CompressionKind
cmprssnReadM = eitherReader $
  \x -> case x of
    "snappy" -> Right SNAPPY
    "zlib"   -> Right ZLIB
    "zstd"   -> Right ZSTD
    "lzo"    -> Right LZO
    _        -> Left ("Unsupported compression kind: " <> x)


main :: IO ()
main = do
  cmnd <- customExecParser (prefs showHelpOnEmpty) (info (parser <**> helper) idm)

  orDie pack $
    case cmnd of
      Json orcIn ->
        printOrcFile orcIn

      Rewrite orcIn orcOut cmprssn ->
        withOrcStripes orcIn $ \_ ->
          putOrcFile cmprssn orcOut . Streaming.map snd

      RoundTrip orcIn orcOut cmprssn chunks ->
        withOrcStream orcIn $ \typ ->
          putOrcFile cmprssn orcOut .
            streamFromLogical chunks typ
