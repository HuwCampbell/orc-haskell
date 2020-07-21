import           Hedgehog.Main

import qualified Test.Orc.Streams.Bytes
import qualified Test.Orc.Streams.Integer
import qualified Test.Orc.Type
import qualified Test.Orc.Logical

main :: IO ()
main =
  defaultMain [
    Test.Orc.Streams.Bytes.tests
  , Test.Orc.Streams.Integer.tests
  , Test.Orc.Type.tests
  , Test.Orc.Logical.tests
  ]
