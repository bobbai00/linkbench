/*
 * Copyright 2012, Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.LinkBench;

import java.util.Arrays;
import java.util.Properties;


public class Link {

  public Link(long id1, long link_type, long id2,
      byte visibility, byte[] data, int version, long time) {
    this.id1 = id1;
    this.link_type = link_type;
    this.id2 = id2;
    this.visibility = visibility;
    this.data = data;
    this.version = version;
    this.time = time;
  }

  Link() {
    link_type = LinkStore.DEFAULT_LINK_TYPE;
    visibility = LinkStore.VISIBILITY_DEFAULT;
  }

  public boolean equals(Object other) {
    if (other instanceof Link) {
      Link o = (Link) other;
      return id1 == o.id1 && id2 == o.id2 &&
          link_type == o.link_type &&
          visibility == o.visibility &&
          version == o.version && time == o.time &&
          Arrays.equals(data, o.data);
    } else {
      return false;
    }
  }

  public Properties toProperties() {
    Properties properties = new Properties();
    properties.setProperty("id1", String.valueOf(id1));
    properties.setProperty("id2", String.valueOf(id2));
    properties.setProperty("link_type", String.valueOf(link_type));
    properties.setProperty("visibility", String.valueOf(visibility));
    properties.setProperty("version", String.valueOf(version));
    properties.setProperty("time", String.valueOf(time));
    properties.setProperty("data", data.toString());

    return properties;
  }

  /**
   * return true if given visibilityVal indicates visible
   * @param visibilityVal
   * @return
   */
  public static boolean checkVisibility(String visibilityVal) {
    int visibility = Integer.getInteger(visibilityVal);
    return visibility == LinkStore.VISIBILITY_DEFAULT;
  }

  public String toString() {
    return String.format("Link(id1=%d, id2=%d, link_type=%d," +
        "visibility=%d, version=%d," +
        "time=%d, data=%s", id1, id2, link_type,
        visibility, version, time, data.toString());
  }

  /**
   * Clone an existing link
   */
  public Link clone() {
    Link l = new Link();
    l.id1 = this.id1;
    l.link_type = this.link_type;
    l.id2 = this.id2;
    l.visibility = this.visibility;
    l.data = this.data.clone();
    l.version = this.version;
    l.time = this.time;
    return l;
  }

  /** The node id of the source of directed edge */
  public long id1;
  static public String ID1 = "id1";

  /** The node id of the target of directed edge */
  public long id2;
  static public String ID2 = "id2";

  /** Type of link */
  public long link_type;
  static public String LINK_TYPE = "link_type";

  /** Visibility mode */
  public byte visibility;
  static public String VISIBILITY = "visibility";

  /** Version of link */
  public int version;
  static public String VERSION = "version";

  /** time is the sort key for links.  Often it contains a timestamp,
      but it can be used as a arbitrary user-defined sort key. */
  public long time;
  static public String TIME = "time";

  /** Arbitrary payload data */
  public byte[] data;
  static public String DATA = "data";

}
