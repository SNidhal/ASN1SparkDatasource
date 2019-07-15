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
package com.beanit.jasn1.ber.types.string;

import com.beanit.jasn1.ber.BerTag;
import com.beanit.jasn1.ber.types.BerOctetString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class BerVideotexString extends BerOctetString {

  public static final BerTag tag =
      new BerTag(BerTag.UNIVERSAL_CLASS, BerTag.PRIMITIVE, BerTag.VIDEOTEX_STRING_TAG);
  private static final long serialVersionUID = 1L;

  public BerVideotexString() {}

  public BerVideotexString(byte[] value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return new String(value);
  }

  @Override
  public int encode(OutputStream reverseOS, boolean withTag) throws IOException {

    int codeLength = super.encode(reverseOS, false);

    if (withTag) {
      codeLength += tag.encode(reverseOS);
    }

    return codeLength;
  }

  @Override
  public int decode(InputStream is, boolean withTag) throws IOException {

    int codeLength = 0;

    if (withTag) {
      codeLength += tag.decodeAndCheck(is);
    }

    codeLength += super.decode(is, false);

    return codeLength;
  }
}
