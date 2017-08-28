# ChangeLog for the Tensei-Data Agent

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## Conventions when editing this file.

Please follow the listed conventions when editing this file:

* one subsection per version
* reverse chronological order (latest entry on top)
* write all dates in iso notation (`YYYY-MM-DD`)
* each version should group changes according to their impact:
    * `Added` for new features.
    * `Changed` for changes in existing functionality.
    * `Deprecated` for once-stable features removed in upcoming releases.
    * `Removed` for deprecated features removed in this release.
    * `Fixed` for any bug fixes.
    * `Security` to invite users to upgrade in case of vulnerabilities.

## Unreleased

## 1.13.3 (2017-06-02)

### Fixed

- crashed transformers cause endless running

## 1.13.2 (2017-05-24)

- no significant changes

## 1.13.1 (2017-05-18)

### Fixed

- usage of `java.io.FileInputStream` and `java.io.FileOutputStream` considered harmful

## 1.13.0 (2017-05-03)

### Changed

- increase default setting for agent memory to 2GB
- restructure sbt configuration
- switch to scalafmt for code formatting
- use stricter compiler flags
- update Scala to 2.11.11

### Removed

- usage of deprecated `BaseApplication` and `Settings` from tensei api

### Fixed

- agent name could be `null`

## 1.12.1 (2017-03-22)

### Fixed

- conversion between different encodings breaks sometimes

## 1.12.0 (2017-03-13)

### Added

- parameter `locale` for `LowerOrUpper` transformer
- support encoding specification for dfasdl schema extractor on local files
- use default encoding from dfasdl schema if no element encoding is set
- transparent conversion of Microsoft Excel files upon schema extraction and processing

### Changed

- update Akka to 2.4.17
- update dfasdl-core and dfasdl-utils to 1.25.0
- update sbt-native-packager to 1.2.0-M8
- update Derby driver to 10.13.1.1
- update Firbird driver to 2.2.12
- update MariaDB driver to 1.5.9
- update PostgreSQL driver to 42.0.0
- update SQLite driver to 3.16.1
- adjusted to changes of the schema extractor parameters

### Removed

- settings and support for encoding detection (It never worked sufficiently.)

### Fixed

- use of lower and uppercase string functions lead to locale specific issues
- `LowerOrUpper` transformer issues related to locale

## 1.11.0 (2017-01-20)

- no significant changes

## 1.10.0 (2017-01-04)

### Changed

- store character data internaly in binary form (42% memory gain!)

## 1.9.2 (2016-11-30)

### Changed

- specify scala version using `in ThisBuild` in sbt
- update sbt-native-packager to 1.2.0-M7
- moved usecase tests to integration tests, use `it:testOnly usecases.*` to execute

## 1.9.1 (2016-11-22)

- no changes

## 1.9.0 (2016-11-10)

### Added

- execute tests before building debian package
- defaults for logback configuration options
- activator binary 1.3.12
- Support for Firebird database

### Changed

- update Akka to 2.4.12
- update MariaDB driver to 1.5.5
- update PostgreSQL driver to 9.4.1212
- update SQLite driver to 3.15.1
- update H2 driver to 1.4.193
- adjust code according to new Akka release
- code cleanup
- `application.ini` contains better defaults
- update SBT to 0.13.13
- update sbt-native-packager to 1.2.0-M5
- update sbt-pgp to 1.0.1
- update sbt-wartremover to 1.2.0
- update ScalaTest to 3.0.0
- cleaned up sbt plugins

### Removed

- xsbt-filter plugin
- custom templates for sbt-native-packager

### Fixed

- `UniqueValueBuffer` does not unsubscribe from event channel after shutdown
- `AutoIncrementValueBuffer` does not unsubscribe from event channel after shutdown
- `NetworkCSVSchemaExtractorTest` does not wait for jetty test server to start
- Tests using sqlite do not run under FreeBSD.
- Broken floating point number parsing on some databases (data corruption!).
- Wrong format descriptions generated for some floating point values and decimals.

## 1.8.0 (2016-06-22)

### Added

- collaboration files
    - [AUTHORS.md](AUTHORS.md)
    - this CHANGELOG file
    - [CONTRIBUTING.md](CONTRIBUTING.md)
    - [LICENSE](LICENSE)
- support normalisation
- extract schema from Derby
- extract schema from MariaDB
- extract primary key, foreign key and auto increment information
from HyperSQL
- extract primary key, foreign key and auto increment information
from Microsoft SQLServer
- date field support for Oracle
- line break support for csv files
- more supported foreign key field types
- support optional fields for foreign key values
- send log files to frontend upon request
- support for `unique` constraints
- transformer for converting datetime values
- check for already existing unique values in a target database
    - **Attention!** This feature works only if the outsourced table
    only has one unique column and a primary key column that is a number
    and created via auto increment.

### Changed

- disable logging to database
- separate logfiles for each run
- added current date and time support to overwrite transformer
- changed internal hashing algorithm to xxHash64
- switch versioning to sbt-git

### Fixed

- use `ElementReference` where ever possible
- cluster client reconnect problems
- crash when extracting H2 database schema
- ignore fulltext search columns for PostgreSQL databases
- stop database writer if an error occurs
- regognise `formatnum` when parsing csv
- cronjob issues
- ORA-00911: invalid character
- csv file with empty lines at the end produce errors
- error resolving foreign key relations
- oracle does not support `getMetaData` and `getParameterType`
- if then else numeric transformer error

## 1.7.0 (2016-03-03)

### Added

- sort recipes and mappings according to foreign key relations
- create database tables according to foreign key relations
- transformer for auto increment values
- transformer for foreign key values
- buffer auto increment values
- create database tables using primary key, foreign key and auto increment
informations from DFASDL
- date type converter transformer
- extract date and time formats for DFASDL generation

### Changed

- extract primary key, foreign keys and auto increment information from
database for DFASDL generation
- adjust prepared statements for auto increments

### Fixed

- relation of `MappingAllToAll` and `MappingOneToOne` not associative
- xml parser crashing on empty elements
- oracle data field issues
- suggester creating too many recipes and mappings
- Oracle-JDBC ORA-01882: timezone region not found
- id transformer broken
- errors on non iso date formats
- use `formatnum` for numbers with leading zeros
- time zone issues in tests
- parsing datetime issues

## 1.6.0 (2016-01-21)

### Added

- SQLite
- respect csv header line

### Changed

- code formatting via scalariform
- complete refactoring of the `Processor`
- disable custom profiling code
- print line number upon `FileParserException`
- refactor message for `ReturnXMLStructure`

### Fixed

- crash on cookbook transformation
- database writer crashing after unsuccessful write operation
- csv files using semicolon
- wrong ids generated during DFASDL extraction
- auto generated name of extracted DFASDL unreadable
- exception when parsing csv files including null values
- invalid unicode characters in extracted DFASDL
- slow parsing

## 1.5.0 (2015-11-30)

### Added

- parser optimisations
- json support

### Fixed

- case sensitivity for JSON attributes
- json name attribute for array elements

## 1.4.2 (2015-10-13)

### Fixed

- Backports of 1.5.0 fixes.

## 1.4.1 (2015-10-12)

### Fixed

- Backports of 1.5.0 fixes.

## 1.4.0 (2015-09-29)

### Added

- parsing of incorrect date fields
- timestamp adjuster transformer
- empty string transformer
- overwrite transformer
- lower/bigger transformer

### Changed

- date converter transformer using milliseconds
- use `None` instead for empty strings

### Fixed

- database writer using `"NULL"` instead of `NULL` for strings

## 1.3.0 (2015-08-27)

### Added

- more network files (ftp, ftps, sftp)
- source data filters
- Count transformer

### Changed

- several refactorings

### Fixed

- Drupal vancode transformer error
- Date converter transformer error
- Replace transformer possible data loss
- Mappings using identical ID fields not supported

## 1.2.0 (2015-08-03)

### Added

- Transformers
    - Drupal vancode
    - `DateTime` to `Timestamp`
    - `Timestamp` to `DateTime`
    - ID generator
- Extract DFASDL from network files
- Microsoft SQLServer support
- Oracle database support
- Issue warning if writer is in closing mode.
- Clustering of the agent, spread saved data accross the cluster
- basic statistics
- recognise `formatnum` and `time` in DFASDL extraction

### Fixed

- ConnectionTimeout upon DFASDL extraction not escalated.
- Race condition in test for statistics
- Error when parsing empty files or database tables
- DFASDL extraction from PostgreSQL databases broken

## 1.1.1 (2015-07-14)

### Fixed

- Backports of 1.2.0 fixes

## 1.1.0 (2015-06-29)

### Added

- Create DFASDL from csv file.
- Test if database tables already exist (MySQL).
- Recognize `formatnum` fields in csv files.
- Replace transformer
- Read numerical values from database.

### Fixed

- NumberFormatException not logged.
- Regular expression for `formatnum` missing escaping.
- Error on missing database connection or insufficient access rights.

## 1.0.0 (2015-06-01)

Initial release.
