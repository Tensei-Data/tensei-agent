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

import akka.testkit.TestActorRef
import com.wegtam.tensei.adt.DFASDL
import com.wegtam.tensei.agent.DataTreeDocumentsManager.DataTreeDocumentsManagerMessages

class DataTreeDocumentsManagerTest extends ActorSpec {
  describe("DataTreeDocumentsManager") {
    describe("when receiving a CreateDataTreeDocumentMessage") {
      it("should create the DataTreeDocument and return it's actor ref and dfasdl") {
        val dtdm =
          TestActorRef(DataTreeDocumentsManager.props(Option("DataTreeDocumentsManagerTest")))
        val dfasdl = DFASDL(
          id = "MY-DFASDL",
          content =
            """<?xml version="1.0" encoding="UTF-8"?><dfasdl xmlns="http://www.dfasdl.org/DFASDL" default-encoding="utf-8" semantic="niem"><seq id="monarchs"><elem id="row"><str id="house" stop-sign=","/><str id="names" stop-sign=","/><str id="title" stop-sign=","/><str id="realm"/></elem></seq></dfasdl>"""
        )

        dtdm ! DataTreeDocumentsManagerMessages.CreateDataTreeDocument(dfasdl)

        val response = expectMsgType[DataTreeDocumentsManagerMessages.DataTreeDocumentCreated]

        response.dfasdl should be(dfasdl)
      }
    }
  }
}
