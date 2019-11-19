import           Hedgehog.Main

import qualified Test.Orc.Streams.Bytes
import qualified Test.Orc.Streams.Integer
import qualified Test.Viking.Stream.Cereal


main :: IO ()
main =
  defaultMain [
    Test.Orc.Streams.Bytes.tests
  , Test.Viking.Stream.Cereal.tests
  ]
