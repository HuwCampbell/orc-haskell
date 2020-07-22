module Main (
  main
) where

import           Control.Monad.IO.Class
import           Control.Monad.Trans.Either.Exit
import           Data.Text (pack)

import           Orc.Serial.Binary.Striped
import           Orc.Serial.Binary.Logical
import           Orc.Schema.Types (CompressionKind (..), types)

import           Options.Applicative

import qualified Streaming.Prelude as Streaming

parser :: Parser Command
parser =
  hsubparser $ mconcat [
    command "json"      (info json      (progDesc "Print and ORC file as JSON rows"))
  , command "reencode"  (info rewrite   (progDesc "Re-write an Orc file with this encoder"))
  , command "roundtrip" (info roundTrip (progDesc "Same as reencode, but going via logical representation"))
  , command "type"      (info typeInfo  (progDesc "Print type or Orc file"))
  ]


data Command
  = Json FilePath
  | Rewrite FilePath FilePath (Maybe CompressionKind)
  | RoundTrip FilePath FilePath (Maybe CompressionKind) Int
  | Type FilePath
  deriving (Eq, Show)


json :: Parser Command
json = Json <$> strArgument (metavar "INPUT" <> help "Orc file to print as JSON")


rewrite :: Parser Command
rewrite =
  Rewrite
    <$> strArgument (metavar "INPUT" <> help "Orc file to read from")
    <*> strArgument (metavar "OUTPUT" <> help "Orc file to write to")
    <*> optional (option compressionReadM (long "compression" <> help "Compression format to use"))


roundTrip :: Parser Command
roundTrip =
  RoundTrip
    <$> strArgument (metavar "INPUT" <> help "Orc file to read from")
    <*> strArgument (metavar "OUTPUT" <> help "Orc file to write to")
    <*> optional (option compressionReadM (long "compression" <> help "Compression format to use"))
    <*> option auto (long "chunk-size" <> Options.Applicative.value 10000 <> help "Number of rows in each stripe" <> showDefault)


typeInfo :: Parser Command
typeInfo = Type <$> strArgument (metavar "INPUT" <> help "Orc file to print as JSON")


compressionReadM :: ReadM CompressionKind
compressionReadM = eitherReader $
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
          putOrcStream typ cmprssn chunks orcOut

      Type orcIn ->
        withOrcFile orcIn $ \(_, _, f) ->
          liftIO $
            print (types f)
