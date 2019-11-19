import           Hedgehog.Main

import qualified Test.Orc.Streams.Bytes
import qualified Test.Orc.Streams.Integer

main :: IO ()
main =
  defaultMain [
    Test.Orc.Streams.Bytes.tests
  , Test.Orc.Streams.Integer.tests
  ]
