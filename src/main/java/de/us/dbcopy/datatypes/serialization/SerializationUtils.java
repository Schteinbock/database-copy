package de.us.dbcopy.datatypes.serialization;

import de.us.dbcopy.exception.DatabaseCopyRuntimeException;

/**
 * Utility class that is used to produce and decode Hex Strings
 * @author Uli Schneider 
 */
class SerializationUtils {
	
	/**
	 * Produces a Hex string for the given entire byte array.
	 * @param bytes
	 * @return
	 */
	public static String bytesToHex(byte bytes[]) {
		return bytesToHex(bytes,bytes.length);
	}
	
	/**
	 * Produces a Hex string for the given byte array. For each Byte two characters of the value 0-9 or A-F is returned.
	 * @return
	 */
	public static String bytesToHex(byte[] bytes,int until) {
		StringBuilder builder = new StringBuilder(bytes.length*2);
		for (int i = 0; i < until; i++) {
			byte b = bytes[i];
			int v=b & 0xff;
			builder.append(getChar(v/16));
			builder.append(getChar(v%16));
		}
		return builder.toString();
	}
	
	private static char getChar(int b) {
		if(b<=9 && b>=0) {
			b+=48;
		} else if(b>9) {
			b+=55;
		} else {
			throw new DatabaseCopyRuntimeException(String.format("Could not convert value to Hex: %d",b));
		}
		return (char)b;
	}
	
	/**
	 * Decodes a HexString to a byte array. It is assumed that the entire {@link String} only contains the characters A-F or 0-9
	 * If {@link String#length()} is odd, the last character of the {@link String} will be omitted.
	 * @param hexString
	 * @return
	 */
	public static byte[] hexToBytes(String hexString) {
		final int length=hexString.length();
		byte[] o=new byte[length/2];
		int pos=0;
		while(length-pos >=2) {
			byte b=(byte) (deserialize(hexString.charAt(pos))*16);
			b+=deserialize(hexString.charAt(++pos));
			o[pos++/2]=b;
		}
		return o;
	}
	
	private static byte deserialize(char buf) {
		int b = (int)buf;
		b-=48;
		if(b>9) {
			b-=7;
			return (byte)b;
		} else {
			return (byte)b;
		}
	}
}
