module Main (
  main
) where

import           Control.Monad.IO.Class

import qualified Orc.Serial.Binary.Base as Base
import           Orc.Schema.Types (CompressionKind (..), types)

import qualified Orc.Logical as Logical
import qualified Orc.Striped as Striped

import           Options.Applicative

import           Data.Version (showVersion)
import           Paths_optimised_row_columnar (version)

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


versioner :: Parser (a -> a)
versioner = infoOption (showVersion version) (long "version" <> help "Show version" <> hidden)


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
    "none"   -> Right NONE
    _        -> Left ("Unsupported compression kind: " <> x)


main :: IO ()
main = do
  cmnd <- customExecParser (prefs showHelpOnEmpty) (info (parser <**> helper <**> versioner) idm)
  case cmnd of
    Json orcIn ->
      Logical.printOrcFile orcIn

    Rewrite orcIn orcOut cmprssn ->
      Base.raisingOrcErrors $
        Striped.withOrcFileLifted orcIn $ \typ ->
          Striped.putOrcFileLifted (Just typ) cmprssn orcOut .
            Streaming.map snd

    RoundTrip orcIn orcOut cmprssn chunks ->
      Logical.withOrcFile orcIn $ \typ ->
        Logical.putOrcFile typ cmprssn chunks orcOut

    Type orcIn ->
      Base.raisingOrcErrors $
        Base.withOrcFileLifted orcIn $ \(_, _, f) ->
          liftIO $
            print (types f)
