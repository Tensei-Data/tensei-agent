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

package com.wegtam.tensei.agent

import com.wegtam.tensei.adt._

class SortTransformationMappingsFunctionsTest
    extends DefaultSpec
    with SortTransformationMappingsFunctions {
  describe("SortTransformationMappingsFunctions") {
    describe("findFirstId") {
      describe("given empty lists") {
        it("should throw an exception") {
          an[NoSuchElementException] should be thrownBy findFirstId(Vector.empty[ElementReference],
                                                                    List.empty[ElementReference])
        }
      }

      describe("given an empty haystack") {
        it("should return -1") {
          val elements =
            List(ElementReference("T", "A"), ElementReference("T", "B"), ElementReference("T", "C"))
          findFirstId(Vector.empty[ElementReference], elements) should be(-1L)
        }
      }

      describe("given an empty element list") {
        it("should throw an exception") {
          val haystack = Vector(ElementReference("T", "A"),
                                ElementReference("T", "B"),
                                ElementReference("T", "C"))
          an[NoSuchElementException] should be thrownBy findFirstId(haystack,
                                                                    List.empty[ElementReference])
        }
      }

      describe("given valid parameters") {
        describe("containing the first haystack element") {
          it("should return 0") {
            val elements = List(ElementReference("T", "B"),
                                ElementReference("T", "C"),
                                ElementReference("T", "A"))
            val haystack = Vector(ElementReference("T", "A"),
                                  ElementReference("T", "B"),
                                  ElementReference("T", "C"))
            findFirstId(haystack, elements) should be(0)
          }
        }

        describe("containing the last haystack element") {
          it("should return the last index") {
            val elements = List(ElementReference("T", "C"))
            val haystack = Vector(ElementReference("T", "A"),
                                  ElementReference("T", "B"),
                                  ElementReference("T", "C"))
            findFirstId(haystack, elements) should be(haystack.length - 1)
          }
        }

        describe("containing only an element from within the haystack") {
          it("should return the index of that element") {
            val elements = List(ElementReference("T", "B"))
            val haystack = Vector(ElementReference("T", "A"),
                                  ElementReference("T", "B"),
                                  ElementReference("T", "C"))
            findFirstId(haystack, elements) should be(haystack.indexOf(elements.head))
          }
        }
      }
    }

    describe("sortAllToAllMappingPairs") {
      describe("given an empty mapping") {
        it("should return the original mapping") {
          val m              = MappingTransformation(List(), List(ElementReference("T", "B")))
          val sortedElements = Vector(ElementReference("T", "A"), ElementReference("T", "B"))
          sortAllToAllMappingPairs(m)(sortedElements) should be(m)
        }
      }

      describe("given an empty sorted element list") {
        it("should return the original mapping") {
          val m =
            MappingTransformation(List(ElementReference("S", "1"), ElementReference("S", "2")),
                                  List(ElementReference("T", "B"), ElementReference("T", "A")))
          val sortedElements = Vector.empty[ElementReference]
          sortAllToAllMappingPairs(m)(sortedElements) should be(m)
        }
      }

      describe("given a proper mapping and sorted element list") {
        it("should sort the mapping pairs correctly") {
          val m = MappingTransformation(
            List(ElementReference("S", "1"),
                 ElementReference("S", "2"),
                 ElementReference("S", "3")),
            List(ElementReference("T", "B"), ElementReference("T", "C"), ElementReference("T", "A"))
          )
          val sortedElements = Vector(ElementReference("T", "A"),
                                      ElementReference("T", "B"),
                                      ElementReference("T", "C"))
          val expectedM = MappingTransformation(
            List(ElementReference("S", "1"),
                 ElementReference("S", "2"),
                 ElementReference("S", "3")),
            List(ElementReference("T", "A"), ElementReference("T", "B"), ElementReference("T", "C"))
          )
          sortAllToAllMappingPairs(m)(sortedElements) should be(expectedM)
        }
      }
    }

    describe("sortOneToOneMappingPairs") {
      describe("given an empty mapping") {
        it("should throw an exception") {
          val m              = MappingTransformation(List(), List(ElementReference("T", "B")))
          val sortedElements = Vector(ElementReference("T", "A"), ElementReference("T", "B"))
          an[IllegalArgumentException] should be thrownBy sortOneToOneMappingPairs(m)(
            sortedElements
          )
        }
      }

      describe("given an empty sorted element list") {
        it("should return the original mapping") {
          val m =
            MappingTransformation(List(ElementReference("S", "1"), ElementReference("S", "2")),
                                  List(ElementReference("T", "B"), ElementReference("T", "A")))
          val sortedElements = Vector.empty[ElementReference]
          sortOneToOneMappingPairs(m)(sortedElements) should be(m)
        }
      }

      describe("given a proper mapping and sorted element list") {
        it("should sort the mapping pairs correctly") {
          val m = MappingTransformation(
            List(ElementReference("S", "1"),
                 ElementReference("S", "2"),
                 ElementReference("S", "3")),
            List(ElementReference("T", "B"), ElementReference("T", "C"), ElementReference("T", "A"))
          )
          val sortedElements = Vector(ElementReference("T", "A"),
                                      ElementReference("T", "B"),
                                      ElementReference("T", "C"))
          val expectedM = MappingTransformation(
            List(ElementReference("S", "3"),
                 ElementReference("S", "1"),
                 ElementReference("S", "2")),
            List(ElementReference("T", "A"), ElementReference("T", "B"), ElementReference("T", "C"))
          )
          sortOneToOneMappingPairs(m)(sortedElements) should be(expectedM)
        }
      }
    }

    describe("sortMappings") {
      describe("given an empty recipe") {
        it("should return the original recipe") {
          val recipe = Recipe(
            id = "RECIPE",
            mode = Recipe.MapAllToAll,
            mappings = List.empty[MappingTransformation]
          )
          val sortedElements = Vector(ElementReference("T", "A"),
                                      ElementReference("T", "B"),
                                      ElementReference("T", "C"))
          sortMappings(recipe)(sortedElements) should be(recipe)
        }
      }

      describe("given an empty sorted element list") {
        it("should return the original recipe") {
          val recipe = Recipe(
            id = "RECIPE",
            mode = Recipe.MapAllToAll,
            mappings = List(
              MappingTransformation(
                List(ElementReference("S", "1"), ElementReference("S", "2")),
                List(ElementReference("T", "A"))
              ),
              MappingTransformation(
                List(ElementReference("S", "3")),
                List(ElementReference("T", "B"), ElementReference("T", "C"))
              )
            )
          )
          val sortedElements = Vector.empty[ElementReference]
          sortMappings(recipe)(sortedElements) should be(recipe)
        }
      }

      describe("given a proper recipe and sorted element list") {
        it("should sort the mapping list correctly") {
          val recipe = Recipe(
            id = "RECIPE",
            mode = Recipe.MapAllToAll,
            mappings = List(
              MappingTransformation(
                List(ElementReference("S", "1"), ElementReference("S", "2")),
                List(ElementReference("T", "C"))
              ),
              MappingTransformation(
                List(ElementReference("S", "3")),
                List(ElementReference("T", "B"), ElementReference("T", "C"))
              ),
              MappingTransformation(
                List(ElementReference("S", "2")),
                List(ElementReference("T", "A"))
              )
            )
          )
          val sortedElements = Vector(ElementReference("T", "A"),
                                      ElementReference("T", "B"),
                                      ElementReference("T", "C"))

          val expectedRecipe = Recipe(
            id = "RECIPE",
            mode = Recipe.MapAllToAll,
            mappings = List(
              MappingTransformation(
                List(ElementReference("S", "2")),
                List(ElementReference("T", "A"))
              ),
              MappingTransformation(
                List(ElementReference("S", "3")),
                List(ElementReference("T", "B"), ElementReference("T", "C"))
              ),
              MappingTransformation(
                List(ElementReference("S", "1"), ElementReference("S", "2")),
                List(ElementReference("T", "C"))
              )
            )
          )

          sortMappings(recipe)(sortedElements) should be(expectedRecipe)
        }
      }
    }

    describe("sortRecipes") {
      describe("given an empty cookbook") {
        it("should return the original cookbook") {
          val cookbook = Cookbook(
            id = "COOKBOOK",
            sources = List.empty[DFASDL],
            target = None,
            recipes = List.empty[Recipe]
          )
          sortRecipes(cookbook) should be(cookbook)
        }
      }

      describe("given a cookbook with an empty target DFASDL") {
        it("should return the original cookbook") {
          val sourceDfasdl = DFASDL(
            id = "S",
            content =
              """
                |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                |  <elem id="sources">
                |    <str id="1" stop-sign=","/>
                |    <str id="2" stop-sign=","/>
                |    <str id="3"/>
                |  </elem>
                |</dfasdl>
                | """.stripMargin
          )
          val recipes = List(
            Recipe(
              id = "RECIPE-01",
              mode = Recipe.MapAllToAll,
              mappings = List(
                MappingTransformation(
                  List(ElementReference("S", "1"), ElementReference("S", "2")),
                  List(ElementReference("T", "C"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "3")),
                  List(ElementReference("T", "B"), ElementReference("T", "C"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "2")),
                  List(ElementReference("T", "B"))
                )
              )
            ),
            Recipe(
              id = "RECIPE-02",
              mode = Recipe.MapOneToOne,
              mappings = List(
                MappingTransformation(
                  List(ElementReference("S", "1"), ElementReference("S", "2")),
                  List(ElementReference("T", "C"), ElementReference("T", "A"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "3"), ElementReference("S", "2")),
                  List(ElementReference("T", "C"), ElementReference("T", "B"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "2")),
                  List(ElementReference("T", "A"))
                )
              )
            )
          )

          val cookbook = Cookbook(
            id = "COOKBOOK",
            sources = List(sourceDfasdl),
            target = None,
            recipes = recipes
          )

          sortRecipes(cookbook) should be(cookbook)
        }
      }

      describe("given a proper cookbook") {
        it("should sort the cookbook recipes and mappings correctly") {
          val sourceDfasdl = DFASDL(
            id = "S",
            content =
              """
                |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                |  <elem id="sources">
                |    <str id="1" stop-sign=","/>
                |    <str id="2" stop-sign=","/>
                |    <str id="3"/>
                |  </elem>
                |</dfasdl>
                | """.stripMargin
          )
          val targetDfasdl = DFASDL(
            id = "T",
            content =
              """
                |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                |  <elem id="targets">
                |    <str id="A" stop-sign=","/>
                |    <str id="B" stop-sign=","/>
                |    <str id="C"/>
                |  </elem>
                |</dfasdl>
                | """.stripMargin
          )
          val recipes = List(
            Recipe(
              id = "RECIPE-01",
              mode = Recipe.MapAllToAll,
              mappings = List(
                MappingTransformation(
                  List(ElementReference("S", "1"), ElementReference("S", "2")),
                  List(ElementReference("T", "C"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "3")),
                  List(ElementReference("T", "B"), ElementReference("T", "C"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "2")),
                  List(ElementReference("T", "B"))
                )
              )
            ),
            Recipe(
              id = "RECIPE-02",
              mode = Recipe.MapOneToOne,
              mappings = List(
                MappingTransformation(
                  List(ElementReference("S", "1"), ElementReference("S", "2")),
                  List(ElementReference("T", "C"), ElementReference("T", "A"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "3"), ElementReference("S", "2")),
                  List(ElementReference("T", "C"), ElementReference("T", "B"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "2")),
                  List(ElementReference("T", "A"))
                )
              )
            )
          )
          val sortedRecipes = List(
            Recipe(
              id = "RECIPE-02",
              mode = Recipe.MapOneToOne,
              mappings = List(
                MappingTransformation(
                  List(ElementReference("S", "2"), ElementReference("S", "1")),
                  List(ElementReference("T", "A"), ElementReference("T", "C"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "2")),
                  List(ElementReference("T", "A"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "2"), ElementReference("S", "3")),
                  List(ElementReference("T", "B"), ElementReference("T", "C"))
                )
              )
            ),
            Recipe(
              id = "RECIPE-01",
              mode = Recipe.MapAllToAll,
              mappings = List(
                MappingTransformation(
                  List(ElementReference("S", "3")),
                  List(ElementReference("T", "B"), ElementReference("T", "C"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "2")),
                  List(ElementReference("T", "B"))
                ),
                MappingTransformation(
                  List(ElementReference("S", "1"), ElementReference("S", "2")),
                  List(ElementReference("T", "C"))
                )
              )
            )
          )

          val cookbook = Cookbook(
            id = "COOKBOOK",
            sources = List(sourceDfasdl),
            target = Option(targetDfasdl),
            recipes = recipes
          )
          val sortedCookbook = cookbook.copy(recipes = sortedRecipes)

          sortRecipes(cookbook) should be(sortedCookbook)
        }
      }

      describe("given a proper cookbook using foreign keys") {
        describe("with one foreign key") {
          it("should sort the cookbook recipes and mappings correctly") {
            val sourceDfasdl = DFASDL(
              id = "S",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="sources">
                  |    <elem id="source">
                  |      <num id="1" stop-sign=","/>
                  |      <str id="2" stop-sign=","/>
                  |      <str id="3"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
            val targetDfasdl =
              DFASDL(
                id = "T",
                content =
                  """
                    |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                    |  <seq id="target1">
                    |    <elem id="target1-row">
                    |      <num id="A" db-column-name="id"/>
                    |      <str id="B" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target2">
                    |    <elem id="target2-row">
                    |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                    |      <str id="D" db-column-name="firstname"/>
                    |      <num id="E" db-column-name="my_name" db-foreign-key="F"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target3">
                    |    <elem id="target3-row">
                    |      <num id="F" db-column-name="id"/>
                    |      <str id="G" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |</dfasdl>
                    | """.stripMargin
              )
            val recipes = List(
              Recipe(
                id = "RECIPE-01",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "B"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "A"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-02",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "D"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3"), ElementReference("S", "2")),
                    List(ElementReference("T", "C"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "D"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-03",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "G"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3")),
                    List(ElementReference("T", "F"))
                  )
                )
              )
            )
            val sortedRecipes =
              List(
                Recipe(
                  id = "RECIPE-01",
                  mode = Recipe.MapAllToAll,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "A"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "B"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-03",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3")),
                      List(ElementReference("T", "F"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "G"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-02",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3"), ElementReference("S", "2")),
                      List(ElementReference("T", "C"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "D"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "D"))
                    )
                  )
                )
              )

            val cookbook = Cookbook(
              id = "COOKBOOK",
              sources = List(sourceDfasdl),
              target = Option(targetDfasdl),
              recipes = recipes
            )
            val sortedCookbook = cookbook.copy(recipes = sortedRecipes)

            sortRecipes(cookbook) should be(sortedCookbook)
          }
        }

        describe("with multiple foreign keys") {
          it("should sort the cookbook recipes and mappings correctly ignoring unused targets") {
            val sourceDfasdl = DFASDL(
              id = "S",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="sources">
                  |    <elem id="source">
                  |      <num id="1" stop-sign=","/>
                  |      <str id="2" stop-sign=","/>
                  |      <str id="3"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
            val targetDfasdl =
              DFASDL(
                id = "T",
                content =
                  """
                    |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                    |  <seq id="target1">
                    |    <elem id="target1-row">
                    |      <num id="A" db-column-name="id"/>
                    |      <str id="B" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target2">
                    |    <elem id="target2-row">
                    |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                    |      <str id="D" db-column-name="firstname" db-foreign-key="A"/>
                    |      <num id="E" db-column-name="my_name" db-foreign-key="F"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target3">
                    |    <elem id="target3-row">
                    |      <num id="F" db-column-name="id"/>
                    |      <str id="G" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target4">
                    |    <elem id="target4-row">
                    |      <num id="H" db-column-name="id"/>
                    |      <str id="I" db-column-name="name"/>
                    |      <num id="J" db-column-name="another_id" db-foreign-key="K"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target5">
                    |    <elem id="target5-row">
                    |      <num id="K" db-column-name="id"/>
                    |      <str id="L" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |</dfasdl>
                    | """.stripMargin
              )
            val recipes = List(
              Recipe(
                id = "RECIPE-01",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "B"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "A"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-02",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "D"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3"), ElementReference("S", "2")),
                    List(ElementReference("T", "C"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "D"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-03",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "G"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3")),
                    List(ElementReference("T", "F"))
                  )
                )
              )
            )
            val sortedRecipes =
              List(
                Recipe(
                  id = "RECIPE-01",
                  mode = Recipe.MapAllToAll,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "A"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "B"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-03",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3")),
                      List(ElementReference("T", "F"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "G"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-02",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3"), ElementReference("S", "2")),
                      List(ElementReference("T", "C"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "D"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "D"))
                    )
                  )
                )
              )

            val cookbook = Cookbook(
              id = "COOKBOOK",
              sources = List(sourceDfasdl),
              target = Option(targetDfasdl),
              recipes = recipes
            )
            val sortedCookbook = cookbook.copy(recipes = sortedRecipes)

            sortRecipes(cookbook) should be(sortedCookbook)
          }
        }

        describe("with multiple foreign keys") {
          it("should sort the cookbook recipes and mappings correctly") {
            val sourceDfasdl = DFASDL(
              id = "S",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="sources">
                  |    <elem id="source">
                  |      <num id="1" stop-sign=","/>
                  |      <str id="2" stop-sign=","/>
                  |      <str id="3"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
            val targetDfasdl =
              DFASDL(
                id = "T",
                content =
                  """
                    |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                    |  <seq id="target1">
                    |    <elem id="target1-row">
                    |      <num id="A" db-column-name="id"/>
                    |      <str id="B" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target2">
                    |    <elem id="target2-row">
                    |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                    |      <str id="D" db-column-name="firstname" db-foreign-key="A"/>
                    |      <num id="E" db-column-name="my_name" db-foreign-key="F"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target3">
                    |    <elem id="target3-row">
                    |      <num id="F" db-column-name="id"/>
                    |      <str id="G" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target4">
                    |    <elem id="target4-row">
                    |      <num id="H" db-column-name="id"/>
                    |      <str id="I" db-column-name="name"/>
                    |      <num id="J" db-column-name="another_id" db-foreign-key="K"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target5">
                    |    <elem id="target5-row">
                    |      <num id="K" db-column-name="id"/>
                    |      <str id="L" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |</dfasdl>
                    | """.stripMargin
              )
            val recipes = List(
              Recipe(
                id = "RECIPE-01",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "B"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "A"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-02",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "D"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3"), ElementReference("S", "2")),
                    List(ElementReference("T", "C"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "D"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-03",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "G"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3")),
                    List(ElementReference("T", "F"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-04",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"),
                         ElementReference("S", "2"),
                         ElementReference("S", "3")),
                    List(ElementReference("T", "H"),
                         ElementReference("T", "I"),
                         ElementReference("T", "J"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-05",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1")),
                    List(ElementReference("T", "L"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "K"))
                  )
                )
              )
            )
            val sortedRecipes =
              List(
                Recipe(
                  id = "RECIPE-01",
                  mode = Recipe.MapAllToAll,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "A"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "B"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-03",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3")),
                      List(ElementReference("T", "F"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "G"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-02",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3"), ElementReference("S", "2")),
                      List(ElementReference("T", "C"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "D"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "D"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-05",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "K"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1")),
                      List(ElementReference("T", "L"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-04",
                  mode = Recipe.MapAllToAll,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "1"),
                           ElementReference("S", "2"),
                           ElementReference("S", "3")),
                      List(ElementReference("T", "H"),
                           ElementReference("T", "I"),
                           ElementReference("T", "J"))
                    )
                  )
                )
              )

            val cookbook = Cookbook(
              id = "COOKBOOK",
              sources = List(sourceDfasdl),
              target = Option(targetDfasdl),
              recipes = recipes
            )
            val sortedCookbook = cookbook.copy(recipes = sortedRecipes)

            sortRecipes(cookbook) should be(sortedCookbook)
          }
        }

        describe("with multiple foreign keys using cross references") {
          it("should sort the cookbook recipes and mappings correctly") {
            val sourceDfasdl = DFASDL(
              id = "S",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="sources">
                  |    <elem id="source">
                  |      <num id="1" stop-sign=","/>
                  |      <str id="2" stop-sign=","/>
                  |      <str id="3"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
            val targetDfasdl =
              DFASDL(
                id = "T",
                content =
                  """
                    |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                    |  <seq id="target1">
                    |    <elem id="target1-row">
                    |      <num id="A" db-column-name="id"/>
                    |      <str id="B" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target2">
                    |    <elem id="target2-row">
                    |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                    |      <str id="D" db-column-name="firstname" db-foreign-key="I"/>
                    |      <num id="E" db-column-name="my_name" db-foreign-key="F"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target3">
                    |    <elem id="target3-row">
                    |      <num id="F" db-column-name="id"/>
                    |      <str id="G" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target4">
                    |    <elem id="target4-row">
                    |      <num id="H" db-column-name="id"/>
                    |      <str id="I" db-column-name="name"/>
                    |      <num id="J" db-column-name="another_id" db-foreign-key="K"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target5">
                    |    <elem id="target5-row">
                    |      <num id="K" db-column-name="id"/>
                    |      <str id="L" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |</dfasdl>
                    | """.stripMargin
              )
            val recipes = List(
              Recipe(
                id = "RECIPE-01",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "B"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "A"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-02",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "D"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3"), ElementReference("S", "2")),
                    List(ElementReference("T", "C"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "D"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-03",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "G"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3")),
                    List(ElementReference("T", "F"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-04",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"),
                         ElementReference("S", "2"),
                         ElementReference("S", "3")),
                    List(ElementReference("T", "H"),
                         ElementReference("T", "I"),
                         ElementReference("T", "J"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-05",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1")),
                    List(ElementReference("T", "L"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "K"))
                  )
                )
              )
            )
            val sortedRecipes =
              List(
                Recipe(
                  id = "RECIPE-01",
                  mode = Recipe.MapAllToAll,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "A"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "B"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-05",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "K"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1")),
                      List(ElementReference("T", "L"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-04",
                  mode = Recipe.MapAllToAll,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "1"),
                           ElementReference("S", "2"),
                           ElementReference("S", "3")),
                      List(ElementReference("T", "H"),
                           ElementReference("T", "I"),
                           ElementReference("T", "J"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-03",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3")),
                      List(ElementReference("T", "F"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "G"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-02",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3"), ElementReference("S", "2")),
                      List(ElementReference("T", "C"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "D"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "D"))
                    )
                  )
                )
              )

            val cookbook = Cookbook(
              id = "COOKBOOK",
              sources = List(sourceDfasdl),
              target = Option(targetDfasdl),
              recipes = recipes
            )
            val sortedCookbook = cookbook.copy(recipes = sortedRecipes)

            sortRecipes(cookbook) should be(sortedCookbook)
          }
        }

        describe("with multiple foreign keys using even more cross references") {
          it("should sort the cookbook recipes and mappings correctly") {
            val sourceDfasdl = DFASDL(
              id = "S",
              content =
                """
                  |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                  |  <seq id="sources">
                  |    <elem id="source">
                  |      <num id="1" stop-sign=","/>
                  |      <str id="2" stop-sign=","/>
                  |      <str id="3"/>
                  |    </elem>
                  |  </seq>
                  |</dfasdl>
                  | """.stripMargin
            )
            val targetDfasdl =
              DFASDL(
                id = "T",
                content =
                  """
                    |<dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem">
                    |  <seq id="target1">
                    |    <elem id="target1-row">
                    |      <num id="A" db-column-name="id"/>
                    |      <str id="B" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target2">
                    |    <elem id="target2-row">
                    |      <num id="C" db-column-name="id" db-auto-inc="true"/>
                    |      <str id="D" db-column-name="firstname" db-foreign-key="I"/>
                    |      <num id="E" db-column-name="my_name" db-foreign-key="F"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target3">
                    |    <elem id="target3-row">
                    |      <num id="F" db-column-name="id"/>
                    |      <str id="G" db-column-name="name" db-foreign-key="L"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target4">
                    |    <elem id="target4-row">
                    |      <num id="H" db-column-name="id"/>
                    |      <str id="I" db-column-name="name"/>
                    |      <num id="J" db-column-name="another_id" db-foreign-key="K"/>
                    |      <str id="J2" db-column-name="yet_another_foreigner" db-foreign-key="G"/>
                    |    </elem>
                    |  </seq>
                    |  <seq id="target5">
                    |    <elem id="target5-row">
                    |      <num id="K" db-column-name="id"/>
                    |      <str id="L" db-column-name="name"/>
                    |    </elem>
                    |  </seq>
                    |</dfasdl>
                    | """.stripMargin
              )
            val recipes = List(
              Recipe(
                id = "RECIPE-01",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "B"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "A"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-02",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"), ElementReference("S", "2")),
                    List(ElementReference("T", "D"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3"), ElementReference("S", "2")),
                    List(ElementReference("T", "C"), ElementReference("T", "E"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "D"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-03",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "G"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "3")),
                    List(ElementReference("T", "F"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-04",
                mode = Recipe.MapAllToAll,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1"),
                         ElementReference("S", "2"),
                         ElementReference("S", "3")),
                    List(ElementReference("T", "H"),
                         ElementReference("T", "I"),
                         ElementReference("T", "J"),
                         ElementReference("T", "J2"))
                  )
                )
              ),
              Recipe(
                id = "RECIPE-05",
                mode = Recipe.MapOneToOne,
                mappings = List(
                  MappingTransformation(
                    List(ElementReference("S", "1")),
                    List(ElementReference("T", "L"))
                  ),
                  MappingTransformation(
                    List(ElementReference("S", "2")),
                    List(ElementReference("T", "K"))
                  )
                )
              )
            )
            val sortedRecipes =
              List(
                Recipe(
                  id = "RECIPE-01",
                  mode = Recipe.MapAllToAll,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "A"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "B"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-05",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "K"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1")),
                      List(ElementReference("T", "L"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-03",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3")),
                      List(ElementReference("T", "F"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "G"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-04",
                  mode = Recipe.MapAllToAll,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "1"),
                           ElementReference("S", "2"),
                           ElementReference("S", "3")),
                      List(ElementReference("T", "H"),
                           ElementReference("T", "I"),
                           ElementReference("T", "J"),
                           ElementReference("T", "J2"))
                    )
                  )
                ),
                Recipe(
                  id = "RECIPE-02",
                  mode = Recipe.MapOneToOne,
                  mappings = List(
                    MappingTransformation(
                      List(ElementReference("S", "3"), ElementReference("S", "2")),
                      List(ElementReference("T", "C"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "1"), ElementReference("S", "2")),
                      List(ElementReference("T", "D"), ElementReference("T", "E"))
                    ),
                    MappingTransformation(
                      List(ElementReference("S", "2")),
                      List(ElementReference("T", "D"))
                    )
                  )
                )
              )

            val cookbook = Cookbook(
              id = "COOKBOOK",
              sources = List(sourceDfasdl),
              target = Option(targetDfasdl),
              recipes = recipes
            )
            val sortedCookbook = cookbook.copy(recipes = sortedRecipes)

            sortRecipes(cookbook) should be(sortedCookbook)
          }
        }
      }
    }
  }
}
