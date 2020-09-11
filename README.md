<div align="center">

# Orc
### Optimised Row Columnar for Haskell

![Haskell CI](https://github.com/HuwCampbell/orc-haskell/workflows/Haskell%20CI/badge.svg)

</div>


This project is a Haskell native reader and writer for the apache
[ORC](https://orc.apache.org/) file format; supporting reading and
writing all types with snappy, zlib, and zstd compression
standards (lzo and lz4 are not currently supported).

We have property based round-tripping tests for Orc files, and golden
tests for the examples from the ORC specification. All files from
the examples given in the ORC repository work, (apart from the LZO
and LZ4 encoded ones). And large files from the tpcds benchmarks are
able to be processed.


License
-------

This project is currently licensed under the Affero General Public License.

If you would like to use this library in a proprietary product, please
reach out to me to discuss licencing.


API
---

We have presented a layered API using a `withFile` pattern. Most users
will want to import `Orc.Logical` and use `withOrcFile` and `putOrcFile`.


Operating Semantics
-------------------

One of the primary use cases for developing this library was to gather
columnar data, which could be used as a C array. As such, we use
`Storable.Vector` for column types, and gather entire stripes into
memory.

This is a different memory model to the C++ and Java versions, which
seek through the files a lot more, but keep less data in memory.
