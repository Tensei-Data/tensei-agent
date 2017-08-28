/*
 * Copyright (C) 2014 - 2017  Contributors as noted in the AUTHORS.md file
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.wegtam.tensei.agent.helpers

import java.util.Locale

import com.wegtam.tensei.agent.DefaultSpec
import com.wegtam.tensei.agent.SchemaExtractor.FormatsFormattime
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable.ListBuffer

class ExtractorHelpersTest extends DefaultSpec with BeforeAndAfterAll with ExtractorHelpers {

  var oldLocale: Option[Locale] = None

  override protected def beforeAll(): Unit = {
    oldLocale = Option(Locale.getDefault)
    val l = Locale.forLanguageTag("de")
    Locale.setDefault(l)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    oldLocale.foreach(l => Locale.setDefault(l))
    super.afterAll()
  }

  describe("ExtractorHelpers") {
    describe("createFormatnumRegex") {
      describe("without length and precision") {
        it("should work") {
          val result = createFormatnumRegex(0, 0)
          result should be("(-?[\\d\\.,⎖]+)")
        }
      }
      describe("without length") {
        it("should work") {
          val result = createFormatnumRegex(0, 4)
          result should be(s"(-?\\d*?\\.\\d{0,4})")
        }
      }
      describe("without precision") {
        it("should work") {
          val result = createFormatnumRegex(20, 0)
          result should be("(-?\\d{1,20})")
        }
      }
      describe("with length and precision") {
        it("should work") {
          val result = createFormatnumRegex(20, 5)
          result should be("(-?\\d{0,15}\\.\\d{0,5})")
        }
      }
      describe("with different decimal separator") {
        it("should work") {
          val result = createFormatnumRegex(20, 5, ",")
          result should be("(-?\\d{0,15},\\d{0,5})")
        }
      }
    }
  }

  describe("parseLong") {
    describe("with an empty String") {
      it("should be false") {
        parseLong("") should be(false)
      }
    }

    describe("with a real String") {
      it("should be false") {
        parseLong("FOO") should be(false)
      }
    }

    describe("with a double and a dot as decimal separator") {
      it("should be false") {
        parseLong("3.22") should be(false)
      }
    }

    describe("with a double and a comma as decimal separator") {
      it("should be false") {
        parseLong("3,22") should be(false)
      }
    }

    describe("with a long") {
      it("should be true") {
        parseLong("3") should be(true)
      }
    }
  }

  describe("parseDouble") {
    describe("with an empty String") {
      it("should be false") {
        parseDouble("") should be(false)
      }
    }

    describe("with a real String") {
      it("should be false") {
        parseDouble("FOO") should be(false)
      }
    }

    describe("with a double and a dot as decimal separator") {
      it("should be true") {
        parseDouble("3.22") should be(true)
      }
    }

    describe("with a double and a comma as decimal separator") {
      it("should be true") {
        parseDouble("3,22") should be(true)
      }
    }

    describe("with a double and a ⎖ as decimal separator") {
      it("should be true") {
        parseDouble("3⎖22") should be(true)
      }
    }

    describe("with a long") {
      it("should be true") {
        parseDouble("3") should be(true)
      }
    }
  }

  describe("parseDate") {
    describe("with an empty String") {
      it("should be false") {
        parseDate("") should be(false)
      }
    }

    describe("with a wrong value") {
      it("should be false") {
        parseDate("am 23.11.2015 um 14:30") should be(false)
      }
    }

    describe("with just a Date value") {
      it("should be true") {
        parseDate("1970-01-01") should be(true)
      }
    }

    describe("with just a Time value") {
      it("should be false") {
        parseDate("14:30:25") should be(false)
      }
    }

    describe("with DateTime values") {
      describe("ISO conform") {
        it("should be false") {
          parseDate("1970-01-01T14:30:25") should be(false)
        }
      }

      describe("not ISO conform") {
        it("should be false") {
          parseDate("1970-01-01 14:30:25") should be(false)
        }
      }
    }
  }

  describe("parseTime") {
    describe("with an empty String") {
      it("should be false") {
        parseTime("") should be(false)
      }
    }

    describe("with a wrong value") {
      it("should be false") {
        parseTime("am 23.11.2015 um 14:30") should be(false)
      }
    }

    describe("with just a Date value") {
      it("should be false") {
        parseTime("1970-01-01") should be(false)
      }
    }

    describe("with just a Time value") {
      it("should be true") {
        parseTime("14:30:25") should be(true)
      }
    }

    describe("with DateTime values") {
      describe("ISO conform") {
        it("should be false") {
          parseTime("1970-01-01T14:30:25") should be(false)
        }
      }

      describe("not ISO conform") {
        it("should be false") {
          parseTime("1970-01-01 14:30:25") should be(false)
        }
      }
    }
  }

  describe("parseTimestamp") {
    describe("with an empty String") {
      it("should be false") {
        parseTimestamp("") should be(false)
      }
    }

    describe("with a wrong value") {
      it("should be false") {
        parseTimestamp("am 23.11.2015 um 14:30") should be(false)
      }
    }

    describe("with just a Date value") {
      it("should be false") {
        parseTimestamp("1970-01-01") should be(false)
      }
    }

    describe("with just a Time value") {
      it("should be false") {
        parseTimestamp("14:30:25") should be(false)
      }
    }

    describe("with DateTime values") {
      describe("ISO conform") {
        it("should be true") {
          parseTimestamp("1970-01-01T14:30:25") should be(true)
        }
      }

      describe("not ISO conform") {
        it("should be true") {
          parseTimestamp("1970-01-01 14:30:25") should be(true)
        }
      }
    }
  }

  describe("determineSeparator") {
    describe("with no entries") {
      it("should return the '.'") {
        val entries = List[String]()
        determineSeparator(entries) should be(None)
      }
    }

    describe("with no separators") {
      it("should return None") {
        val entries = List[String]("1200", "122", "1200", "1000", "1234567", "1200")
        determineSeparator(entries) should be(None)
      }
    }

    describe("with a thousand separator") {
      it("should return None") {
        val entries = List[String]("1.200", "122", "1.2001", "1000,23", "1.234.567", "1.200")
        determineSeparator(entries) should be(None)
      }
    }

    describe("with the comma as decimal separator") {
      describe("without thousand separator") {
        it("should return the ','") {
          val entries =
            List[String]("1200,20", "1,22", "1200,11", "1000,23", "1234567,89", "100,00")
          determineSeparator(entries) should be(Option(","))
        }
      }

      describe("with thousand separator") {
        it("should return the ','") {
          val entries =
            List[String]("1.200,20", "1,22", "1.200,11", "4.000,23", "1.234.567,89", "1.200,00")
          determineSeparator(entries) should be(Option(","))
        }
      }
    }

    describe("with the dot as decimal separator") {
      describe("without thousand separator") {
        it("should return the '.'") {
          val entries =
            List[String]("1200.20", "1.22", "1200.11", "4000.23", "1234567.89", "1200.00")
          determineSeparator(entries) should be(Option("."))
        }
      }

      describe("with thousand separator") {
        it("should return the '.'") {
          val entries =
            List[String]("1,200.20", "1.22", "1,200.11", "4,000.23", "1,234,567.89", "1,200.00")
          determineSeparator(entries) should be(Option("."))
        }
      }
    }
  }

  describe("determinePrecisionLength") {
    describe("with an empty list") {
      it("should return None") {
        val entries = List[String]()
        determinePrecisionLength(entries, ".") should be(None)
      }
    }

    describe("with a wrong decimal separator") {
      it("should return None") {
        val entries =
          List[String]("1.200,20", "1,22", "1.200,11", "4.000,23", "1.234.567,89", "1.200,00")
        determinePrecisionLength(entries, ".") should be(None)
      }
    }

    describe("with a correct decimal separator") {
      describe("with unequal decimal precisions") {
        describe("with max 2") {
          it("should return 2") {
            val entries =
              List[String]("1.200,0", "1,2", "1.200,11", "4.000,3", "1.234.567,89", "1.200,00")
            determinePrecisionLength(entries, ",") should be(Option(2))
          }
        }

        describe("with max 3") {
          it("should return 3") {
            val entries = List[String]("1.200,20",
                                       "1,223",
                                       "1.200,11",
                                       "4.000,23",
                                       "1.234.567,893",
                                       "1.200,00")
            determinePrecisionLength(entries, ",") should be(Option(3))
          }
        }
      }

      describe("with equal decimal precsions") {
        describe("with length 2") {
          it("should return 2") {
            val entries =
              List[String]("1.200,20", "1,23", "1.200,11", "4.000,23", "1.234.567,93", "1.200,00")
            determinePrecisionLength(entries, ",") should be(Option(2))
          }
        }

        describe("with length 4") {
          it("should return 4") {
            val entries = List[String]("1.200,2077",
                                       "1,2237",
                                       "1.200,1771",
                                       "4.000,2773",
                                       "1.234.567,7893",
                                       "1.200,7700")
            determinePrecisionLength(entries, ",") should be(Option(4))
          }
        }
      }
    }
  }

  describe("parseFormattedTime") {
    describe("with an empty source and format") {
      it("should return false") {
        parseFormattedTime("", "") should be(false)
      }
    }
    describe("with an empty source") {
      it("should return false") {
        parseFormattedTime("", "dd.MM.yyyy") should be(false)
      }
    }
    describe("with an empty format") {
      it("should return false") {
        parseFormattedTime("12.01.2006", "") should be(false)
      }
    }

    describe("for date formats") {
      describe("with format `dd.MM.yyyy`") {
        it("should return true") {
          parseFormattedTime("12.06.2006", "dd.MM.yyyy") should be(true)
        }
      }
      describe("with format `dd MM yyyy`") {
        it("should return true") {
          parseFormattedTime("12 06 2006", "dd MM yyyy") should be(true)
        }
      }
      describe("with format `dd.LLL.yyyy`") {
        it("should return true") {
          parseFormattedTime("12.Jul.2006", "dd.LLL.yyyy") should be(true)
        }
      }
      describe("with format `dd LLL yyyy`") {
        it("should return true") {
          parseFormattedTime("12 Jul 2006", "dd LLL yyyy") should be(true)
        }
      }
      describe("with format `dd/MM/yyyy`") {
        it("should return true") {
          parseFormattedTime("14/06/2006", "dd/MM/yyyy") should be(true)
        }
      }
      describe("with format `dd/LLL/yyyy`") {
        it("should return true") {
          parseFormattedTime("14/Nov/2006", "dd/LLL/yyyy") should be(true)
        }
      }
      describe("with format `MM/dd/yyyy`") {
        it("should return true") {
          parseFormattedTime("11/14/2006", "MM/dd/yyyy") should be(true)
        }
      }
      describe("with format `LLL/dd/yyyy`") {
        it("should return true") {
          parseFormattedTime("Nov/14/2006", "LLL/dd/yyyy") should be(true)
        }
      }
      describe("with format `yyyyMMdd`") {
        it("should return true") {
          parseFormattedTime("20060323", "yyyyMMdd") should be(true)
        }
      }
    }
    describe("for time formats") {
      describe("with format `HH:mm`") {
        it("should return true") {
          parseFormattedTime("14:12", "HH:mm") should be(true)
        }
      }
      describe("with format `HH:mm a` with `am`") {
        it("should return true") {
          parseFormattedTime("2:12 AM", "h:mm a") should be(true)
        }
      }
      describe("with format `HH:mm a` with `pm`") {
        it("should return true") {
          parseFormattedTime("1:12 PM", "h:mm a") should be(true)
        }
      }
    }
    describe("for timestamp formats") {
      describe("with format `yyyy-MM-dd h:mm:ss a` with `am`") {
        it("should return true") {
          parseFormattedTime("2012-01-12 1:12:00 AM", "yyyy-MM-dd h:mm:ss a") should be(true)
        }
      }
      describe("with format `yyyy-MM-dd h:mm:ss a` with `pm`") {
        it("should return true") {
          parseFormattedTime("2012-01-12 1:12:00 PM", "yyyy-MM-dd h:mm:ss a") should be(true)
        }
      }
      describe("with format `yyyy-MM-dd h:mm:ss a` with `am` and zoned-name") {
        it("should return true") {
          parseFormattedTime("2012-01-12 1:12:00 AM UTC", "yyyy-MM-dd h:mm:ss a z") should be(true)
        }
      }
      describe("with format `yyyy-MM-dd h:mm:ss a` with `pm` and zoned-name") {
        it("should return true") {
          parseFormattedTime("2012-01-12 1:12:00 PM UTC", "yyyy-MM-dd h:mm:ss a z") should be(true)
        }
      }
      describe("with format `EEE, dd LLL yyyy HH:mm:ss z`") {
        it("should return true") {
          parseFormattedTime("Mo, 15 Feb 2016 18:18:39 UTC", "EEE, dd LLL yyyy HH:mm:ss z") should be(
            true
          )
        }
      }
    }
  }

  describe("determineSpecificFormat") {
    val formatsFormattime = FormatsFormattime(
      timestamp = List(
        "yyyy-MM-dd h:mm:ss a",
        "yyyy-MM-dd h:mm:ss a z",
        "EEE, dd LLL yyyy HH:mm:ss z"
      ),
      date = List(
        "yyyyMMdd",
        "dd.MM.yyyy",
        "dd MM yyyy",
        "dd.LLL.yyyy",
        "dd LLL yyyy",
        "dd/MM/yyyy",
        "dd/LLL/yyyy",
        "MM/dd/yyyy",
        "LLL/dd/yyyy"
      ),
      time = List(
        "h:mm a",
        "HH:mm"
      )
    )
    describe("timestamps") {
      describe("for `yyyy-MM-dd h:mm:ss a`") {
        describe("all given values fit the pattern") {
          it("should return the `yyyy-MM-dd h:mm:ss a` format") {
            val values =
              ListBuffer("2012-12-12 1:12:22 AM", "2001-01-22 7:22:22 PM", "1985-11-01 4:00:00 AM")
            determineSpecificFormat(values, formatsFormattime.timestamp) should be(
              Option("yyyy-MM-dd h:mm:ss a")
            )
          }
        }
        describe("not all given values fit the pattern") {
          it("should return None") {
            val values =
              ListBuffer("2012-12-12 1:12:22 AM", "2001-01-22 7:22:22 PM", "1985-11-01 4:00:00")
            determineSpecificFormat(values, formatsFormattime.timestamp) should be(None)
          }
        }
      }
    }
    describe("date") {
      describe("for `yyyyMMdd`") {
        describe("all given values fit the pattern") {
          it("should return the `yyyyMMdd` format") {
            val values = ListBuffer("20120112", "20130322", "20011201")
            determineSpecificFormat(values, formatsFormattime.date) should be(Option("yyyyMMdd"))
          }
        }
        describe("not all given values fit the pattern") {
          it("should return None") {
            val values = ListBuffer("20120112", "2013032", "20011201")
            determineSpecificFormat(values, formatsFormattime.date) should be(None)
          }
        }
        describe("not all given values fit the pattern and contain timestamp") {
          it("should return None") {
            val values = ListBuffer("20120112", "20130321 14:12:00", "20011201")
            determineSpecificFormat(values, formatsFormattime.date) should be(None)
          }
        }
      }
    }
    describe("time") {
      describe("for `HH:mm`") {
        describe("all given values fit the pattern") {
          it("should return the `HH:mm` format") {
            val values = ListBuffer("12:12", "22:22", "07:35", "21:15")
            determineSpecificFormat(values, formatsFormattime.time) should be(Option("HH:mm"))
          }
        }
        describe("all given values fit the pattern") {
          it("should return None") {
            val values = ListBuffer("12:12", "22:22:12", "07:35", "21:15")
            determineSpecificFormat(values, formatsFormattime.time) should be(None)
          }
        }
      }
    }
  }
}
