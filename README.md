<div align="center">

# Optimised Row Columnar
### Haskell

![Haskell CI](https://github.com/HuwCampbell/orc-haskell/workflows/Haskell%20CI/badge.svg)

</div>


The project is a Haskell native reader and writer for the apache
[ORC](https://orc.apache.org/) file format; supporting reading and
writing all types with snappy, zlib, and zstd compression
standards (lzo and lz4 are not currently supported).


API
---

Modules occasionally share function names, and may require qualified
imports; for example, when streaming a file of rows from an orc
file, one would import `Orc.Logical` as `Logical` and use
`Logical.withOrcFile`.

The API is based around the
[streaming](http://hackage.haskell.org/package/streaming) library,
using a with pattern for resource handling. This means one can
stream through an ORC file, consuming it as they please, and the
library will take care of seeking to the correct locations and
closing its file handle when done.

Here's an example to count the number of rows in a file.

```haskell
import           Control.Monad.Trans.Except (runExceptT)
import qualified Orc.Serial.Binary.Logical as Logical
import qualified Streaming.Prelude as Streaming

Logical.withOrcFile  "decimal.orc" $ \_orcType ->
  Streaming.length
```

While to count the number of stripes in the file, one would use

```haskell
import qualified Orc.Serial.Binary.Striped as Striped

runExceptT $
  Striped.withOrcFileLifted  "decimal.orc" $ \_orcType ->
    Streaming.length
```

Rows and tables are strongly typed into Haskell algebraic data
types.

Error Handling
--------------

One can choose to explicitly handle errors in the Orc file with
an `ExceptT` monad transformer, or throw exceptions.

The _lifted_ family of functions permit this choice to be made
explicitly, while the unlifted functions for Logical data types
throw exceptions.


Tests
-----

Currently, we have property based round-tripping tests for Orc
files, and golden tests for the examples from the ORC specification.
All files from the examples given in the ORC repository work, (apart
from the LZO and LZ4 encoded ones).
