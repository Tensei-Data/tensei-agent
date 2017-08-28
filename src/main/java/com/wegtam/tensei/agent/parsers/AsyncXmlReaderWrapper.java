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

package com.wegtam.tensei.agent.parsers;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import com.fasterxml.aalto.AsyncByteArrayFeeder;
import com.fasterxml.aalto.AsyncXMLStreamReader;

/**
 * This code was adapted from the unit test package of the aalto-xml suite.
 */
public class AsyncXmlReaderWrapper {
    private final AsyncXMLStreamReader<AsyncByteArrayFeeder> _streamReader;
    private final byte[] _xml;
    private final int _bytesPerFeed;
    private int _offset = 0;

    public AsyncXmlReaderWrapper(AsyncXMLStreamReader<AsyncByteArrayFeeder> sr, String xmlString) {
        this(sr, 1, xmlString);
    }

    public AsyncXmlReaderWrapper(AsyncXMLStreamReader<AsyncByteArrayFeeder> sr, int bytesPerCall, String xmlString)
    {
        _streamReader = sr;
        _bytesPerFeed = bytesPerCall;
        try {
            _xml = xmlString.getBytes("UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getOffset() {
        return _offset;
    }

    public String currentText() throws XMLStreamException {
        return _streamReader.getText();
    }

    public int currentToken() throws XMLStreamException {
        return _streamReader.getEventType();
    }

    public int nextToken() throws XMLStreamException
    {
        int token;

        while ((token = _streamReader.next()) == AsyncXMLStreamReader.EVENT_INCOMPLETE) {
            AsyncByteArrayFeeder feeder = _streamReader.getInputFeeder();
            if (!feeder.needMoreInput()) {
                System.err.println("Got EVENT_INCOMPLETE, could not feed more input!");
                throw new RuntimeException("Got EVENT_INCOMPLETE, could not feed more input!");
            }
            if (_offset >= _xml.length) { // end-of-input?
                feeder.endOfInput();
            } else {
                int amount = Math.min(_bytesPerFeed, _xml.length - _offset);
                feeder.feedInput(_xml, _offset, amount);
                _offset += amount;
            }
        }
        return token;
    }
}
