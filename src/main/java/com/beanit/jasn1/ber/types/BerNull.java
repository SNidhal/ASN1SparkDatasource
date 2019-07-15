/*
 * Copyright 2012 The jASN1 Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.beanit.jasn1.ber.types;

import com.beanit.jasn1.ber.BerLength;
import com.beanit.jasn1.ber.BerTag;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class BerNull implements Serializable, BerType {

  public static final BerTag tag =
      new BerTag(BerTag.UNIVERSAL_CLASS, BerTag.PRIMITIVE, BerTag.NULL_TAG);
  private static final long serialVersionUID = 1L;
  public byte[] code = null;

  public BerNull() {}

  public BerNull(byte[] code) {}

  @Override
  public int encode(OutputStream reverseOS) throws IOException {
    return encode(reverseOS, true);
  }

  public int encode(OutputStream reverseOS, boolean withTag) throws IOException {

    int codeLength = BerLength.encodeLength(reverseOS, 0);

    if (withTag) {
      codeLength += tag.encode(reverseOS);
    }

    return codeLength;
  }

  @Override
  public int decode(InputStream is) throws IOException {
    return decode(is, true);
  }

  public int decode(InputStream is, boolean withTag) throws IOException {

    int codeLength = 0;

    if (withTag) {
      codeLength += tag.decodeAndCheck(is);
    }

    BerLength length = new BerLength();
    codeLength += length.decode(is);

    if (length.val != 0) {
      throw new IOException("Decoded length of BerNull is not correct");
    }

    return codeLength;
  }

  @Override
  public String toString() {
    return "ASN1_NULL";
  }
}
