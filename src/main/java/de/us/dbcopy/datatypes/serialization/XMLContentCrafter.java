package de.us.dbcopy.datatypes.serialization;

import javax.xml.stream.XMLStreamException;

@FunctionalInterface
public interface XMLContentCrafter {
	public void craft(String content) throws XMLStreamException;
}
