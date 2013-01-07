package org.apache.lucene.util;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static sun.reflect.ReflectionFactory.getReflectionFactory;

/**
 * Runtime assertion utility that cannot be disabled with the usage of Java / JDK
 * flags. This might seem redundant, but this class is able to make it easier to quickly test
 * and guarantee invariants and similar conditions within code, making it easier to develop
 * code that fails fast, and cleanly. <br />
 *
 * This class is similar to commons-lang validate, as well as the Preconditions class from google guava,
 * however, in difference to most versions, this is able to throw any arbitrary exception as a cause.
 */
public final class Guarantee {

  // Empty class array, designed such that the JVM can avoid
  // leaks related to empty array sentinels
  private static final Class[] EMPTY_CLASS_ARRAY = new Class[0];

  private Guarantee() {}

  private static final Constructor JAVA_LANG_OBJECT_CONSTRUCTOR;

  ///CLOVER:OFF
  static {
    try {
      JAVA_LANG_OBJECT_CONSTRUCTOR = Object.class.getConstructor((Class[]) null);
    } catch (NoSuchMethodException e) {
      throw new UnknownError("Missing the intrinsic constructor ?");
    }
  }
  ///CLOVER:ON

  /**
   * Guarantees that the given object is not null
   * @param reference the object to check
   * @param message message used if the object is null
   * @throws IllegalArgumentException if the object is null
   */
  public static void ensureNotNull(Object reference, String message) throws IllegalArgumentException {
    if(reference == null) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Guarantees that the given object is not null
   * @param reference the object to check
   * @param message message used if the object is null
   * @param formatArgs arguments used for String.format calls (will only occur if any error happens)
   * @throws IllegalArgumentException if the object is null
   */
  public static void ensureNotNull(Object reference, String message, Object... formatArgs) throws IllegalArgumentException {
    if(reference == null) {
      throw new IllegalArgumentException(format(message, formatArgs));
    }
  }

  /**
   * Guarantees that the given object is not null
   * @param reference the object to check
   * @param throwable the exception to throw if the object is null
   * @param args arguments used to construct the exception
   * @throws T the given exception to be thrown if the object is null
   */
  public static<T extends Throwable> void ensureNotNull(Object reference, Class<T> throwable, Object... args) throws T {
    if(reference == null) {
      throw createException(throwable, args);
    }
  }

  /**
   * Guarantees that the given object is null, useful for checking sentinals
   * @param reference the object to check
   * @param message the message to be used if the object is not null
   * @throws IllegalArgumentException if the object is not null
   */
  public static void ensureIsNull(Object reference, String message) throws IllegalArgumentException {
    if(reference != null) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Guarantees that the given object is null, useful for checking sentinels
   * @param reference the object to check
   * @param message the message to be used if the object is not null
   * @param formatArgs arguments used for String.format calls (will only occur if any error happens)
   * @throws IllegalArgumentException if the object is not null
   */
  public static void ensureIsNull(Object reference, String message, Object... formatArgs) throws IllegalArgumentException {
    if(reference != null) {
      throw new IllegalArgumentException(format(message, formatArgs));
    }
  }

  /**
   * Guarantees that the given object is null, useful for checking sentinels
   * @param reference the object to check
   * @param throwable the exception to throw if the object is not null
   * @param args arguments used to construct the exception
   * @throws T the given exception to be throw if the object is not null
   */
  public static<T extends Throwable> void ensureIsNull(Object reference, Class<T> throwable, Object... args) throws T {
    if(reference != null) {
      throw createException(throwable, args);
    }
  }

  /**
   * Guarantees that the given expression evaluates to true. For example <br />
   * <code>
   *  Assert.ensureIsTrue(facets.size > 30, "The number of product results must be more than 30");
   * </code>
   * @param expression the expression to check for truth
   * @param message the message to use if the expression is false
   * @throws IllegalArgumentException if the given expression is false
   */
  public static void ensureIsTrue(boolean expression, String message) throws IllegalArgumentException {
    if(!expression) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Guarantees that the given expression evaluates to true. For example <br />
   * <code>
   *  Assert.ensureIsTrue(facets.size > 30, "Product results were [%d] wayyy more than 30!", facets.size);
   * </code>
   * @param expression the expression to check for truth
   * @param message the message to use if the expression is false
   * @param formatArgs arguments used for String.format calls (will only occur if any error happens)
   * @throws IllegalArgumentException if the given expression is false
   */
  public static void ensureIsTrue(boolean expression, String message, Object... formatArgs) throws IllegalArgumentException {
    if(!expression) {
      throw new IllegalArgumentException(format(message, formatArgs));
    }
  }

  /**
   * Guarantees that the given expression evaluates to true. For example <br />
   * <code>
   *  Assert.ensureIsTrue(facets.size > 30, ServiceInvocationException.class, "The number of facets must be > 30");
   * </code>
   * @param expression the expression to check for truth
   * @param throwable the exception to throw is the expression is false
   * @param args arguments used to construct the exception
   * @throws T the given exception to be throw if the expression is false
   */
  public static<T extends Throwable> void ensureIsTrue(boolean expression, Class<T> throwable, Object... args) throws T {
    if(!expression) {
      throw createException(throwable, args);
    }
  }

  /**
   * Guarantees that the given expression evaluates to false. For example <br />
   * <code>
   *  Assert.ensureIsFalse(facets.size > 30, "The number of facets must be less than 30");
   * </code>
   * @param expression the expression to check for falsehood
   * @param message the message to use is the expression is true
   * @throws IllegalArgumentException if the expression is true
   */
  public static void ensureIsFalse(boolean expression, String message) throws IllegalArgumentException {
    if(expression) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Guarantees that the given expression evaluates to false. For example <br />
   * <code>
   *  Assert.ensureIsFalse(facets.size > 30, "Facet results were [%s] way more than 30!", facets.size);
   * </code>
   * @param expression the expression to check for falsehood
   * @param message the message to use is the expression is true
   * @param formatArgs arguments used for String.format calls (will only occur if any error happens)
   * @throws IllegalArgumentException if the expression is true
   */
  public static void ensureIsFalse(boolean expression, String message, Object... formatArgs) throws IllegalArgumentException {
    if(expression) {
      throw new IllegalArgumentException(format(message, formatArgs));
    }
  }

  /**
   * Guarantees that the given expression evaluates to false. For example <br />
   * <code>
   *  Assert.ensureIsFalse(facets.size > 30, ServiceInvocationException.class, "The number of facets must be < 30");
   * </code>
   * @param expression the expression to check for falsehood
   * @param throwable the exception to throw is the expression is true
   * @param args arguments used to construct the exception
   * @throws T the given exception to be throw if the expression is true
   */
  public static<T extends Throwable> void ensureIsFalse(boolean expression, Class<T> throwable, Object... args) throws T {
    if(expression) {
      throw createException(throwable, args);
    }
  }

  /**
   * Guarantees that the given object is an instance of the given type
   * @param reference the reference to test
   * @param type the type to assert the reference against
   * @param message the message to use if the object is not an instance of the given type
   * @throws IllegalArgumentException if the reference is not of the given type
   */
  public static void ensureInstanceOf(Object reference, Class type, String message) throws IllegalArgumentException {
    ensureNotNull(type, "type is null");
    if(!(type.isInstance(reference))) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Guarantees that the given object is an instance of the given type
   * @param reference the reference to test
   * @param type the type to assert the reference against
   * @param message the message to use if the object is not an instance of the given type
   * @param formatArgs arguments used for String.format calls (will only occur if any error happens)
   * @throws IllegalArgumentException if the reference is not of the given type
   */
  public static void ensureInstanceOf(Object reference, Class type, String message, Object... formatArgs) throws IllegalArgumentException {
    ensureNotNull(type, "type is null");
    if(!(type.isInstance(reference))) {
      throw new IllegalArgumentException(format(message, formatArgs));
    }
  }

  /**
   * Guarantees that the given object is an instance of the given type
   * @param reference the reference to test
   * @param type the type to assert the reference against
   * @param throwable the exception to throw if the reference is not of the given type
   * @param args arguments used to construct the exception
   * @throws T the given exception to be throw if the reference is not of the given type
   */
  public static<T extends Throwable> void ensureInstanceOf(Object reference, Class type, Class<T> throwable, Object... args) throws T {
    ensureNotNull(type, "type is null");
    if(!(type.isInstance(reference))) {
      throw createException(throwable, args);
    }
  }

  /**
   * Guarantees that the given string is not null, empty, or pure whitespace. For instance all the following are
   * invalid <br />
   * <code>
   *  Assert.ensureNotBlank("      ", "String should not be blank");
   *  Assert.ensureNotBlank(null, "String should not be null");
   *  Assert.ensureNotBlank("", "String should not be empty");
   * </code>
   * @param string the string to check over
   * @param message the message to use if the string is blank
   * @throws IllegalArgumentException if the string is blank
   */
  public static void ensureNotBlank(CharSequence string, String message) throws IllegalArgumentException {
    if(isBlank(string)) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Guarantees that the given string is not null, empty, or pure whitespace. For instance all the following are
   * invalid <br />
   * <code>
   *  Assert.ensureNotBlank(facets.getUrl(), "URL for [%s] should not be blank", facets.getName());
   * </code>
   * @param string the string to check over
   * @param message the message to use if the string is blank
   * @param formatArgs arguments used for String.format calls (will only occur if any error happens)
   * @throws IllegalArgumentException if the string is blank
   */
  public static void ensureNotBlank(CharSequence string, String message, Object... formatArgs) throws IllegalArgumentException {
    if(isBlank(string)) {
      throw new IllegalArgumentException(format(message, formatArgs));
    }
  }

  /**
   * Guarantees that the given string is not null, empty, or pure whitespace. For instance all of the following
   * are invalid <br />
   * <code>
   *  Assert.ensureNotBlank("      ", ServiceInvocationException.class, "String should not be blank");
   *  Assert.ensureNotBlank(null, ServiceInvocationException.class, "String should not be null");
   *  Assert.ensureNotBlank("", ServiceInvocationException.class, "String should not be empty");
   * </code>
   * @param string the string to check over
   * @param throwable the exception to throw if the reference is not of the given type
   * @param args arguments used to construct the exception
   * @throws T the given exception to be throw if the reference is not of the given type
   */
  public static<T extends Throwable> void ensureNotBlank(CharSequence string, Class<T> throwable, Object... args) throws T {
    if(isBlank(string)) {
      throw createException(throwable, args);
    }
  }

  // Implementation helper methods follow
  //------------------------------------------------------------------------------------------

  /**
   * Attempts to create an exception as wanted by various assert lines, will attempt to construct with
   * as many of the given args as makes sense, if this fails it will construct a default object, potentially
   * using trickery
   * @param throwable the class to construct
   * @param args constructor params to attempt to honor
   * @return instance of the throwable if possible
   */
  private static <T extends Throwable> T createException(Class<T> throwable, Object[] args) {
    try {
      List<Constructor<T>> cons = null;
      for(int size=args.length; size > 0; size--) {
        Object[] workingArgs = Arrays.copyOf(args, size);
        cons = obtainPossibleCtors(throwable, collectTypes(workingArgs));
        if (!cons.isEmpty()) {
          Constructor<T> con = cons.get(0);
          if(!con.isAccessible()) {
            con.setAccessible(true);
          }
          T instance = throwableOrNull(con, workingArgs);
          if(instance != null) {
            return instance;
          }
        }
      }
      Constructor<T> mungedConstructor = getReflectionFactory()
          .newConstructorForSerialization(throwable, JAVA_LANG_OBJECT_CONSTRUCTOR);
      mungedConstructor.setAccessible(true);
      return throwableOrNull(mungedConstructor);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Given an array of objects (essentially anything that
   * can exist in a var args call), this method will collect
   * the types of these objects into a new array, designed to be
   * suitable for deducing ctor, and method handles.
   * @param args the arguments to collect
   * @return array of Class types for the given arguments, empty
   * array if the array is empty, or null.
   */
  public static Class[] collectTypes(Object... args) {
    if(args == null || args.length == 0) {
      return EMPTY_CLASS_ARRAY;
    }

    boolean deadArray = true;
    Class[] toReturn = new Class[args.length];
    for(int i=0; i<args.length;i++) {
      Object o = args[i];
      deadArray &= (o == null);
      toReturn[i] = o != null ? args[i].getClass() : null;
    }
    return deadArray ? EMPTY_CLASS_ARRAY : toReturn;
  }

  private static <T> List<Constructor<T>> obtainPossibleCtors(Class<T> clazz, Class... types) {
    // Do not depend on Assert to avoid circular deps
    if(clazz == null) {
      throw new RuntimeException("Must have a class to deduce ctors for");
    }
    try {
      List<Constructor<T>> toReturn = new ArrayList<Constructor<T>>();

      if(types == null) {
        return Arrays.asList(clazz.getConstructor(EMPTY_CLASS_ARRAY));
      } else {
        boolean emptySentinals = false;
        for (final Class type1 : types) {
          if (type1 == null) {
            emptySentinals = true;
            break;
          }
        }

        // We can attempt to find the best ctor on the fast path
        if(!emptySentinals) {
          // We got lucky and got the right ctor right away
          try {
            Constructor<T> ctor = clazz.getConstructor(types);
            if(ctor != null) {
              return Arrays.asList(ctor);
            }
          } catch(NoSuchMethodException nsm) {
            // pass
          }
        }

        extractFromDeclaredConstructors(clazz, toReturn, types);

        return toReturn;
      }
    } catch (Exception e) {
      throw new RuntimeException("Error deducing constructors", e);
    }
  }

  private static <T> void extractFromDeclaredConstructors(Class<T> clazz, List<Constructor<T>> toReturn, Class[] types) {
    Constructor[] ctors = clazz.getDeclaredConstructors();
    for(Constructor ctor : ctors) {
      Class[] ctorTypes = ctor.getParameterTypes();
      if(ctorTypes.length == types.length) {
        boolean match = true;
        for(int i=0; i < ctorTypes.length; i++) {
          final Class type = types[i];
          final Class ctorType = ctorTypes[i];

          if(type != null && !ctorType.equals(type)) {
            match = false;
            break;
          }
        }
        if(match) {
          toReturn.add(ctor);
        }
      }
    }
  }

  /**
   * Elides exception handling around the construction of exceptions
   * @param cons the constructor to call newInstance upon
   * @param args the constructor params to attempt to honor
   * @return instance if construction was successful, null otherwise
   */
  private static <T extends Throwable> T throwableOrNull(Constructor<T> cons, Object... args) {
    try {
      return cons.newInstance(args);
    } catch (Exception e) {
      return null;
    }
  }

  private static boolean isBlank(CharSequence chars) {
    if(chars == null || chars.length() == 0) {
      return true;
    }

    for(int i=0; i<chars.length(); i++) {
      if(!Character.isWhitespace(chars.charAt(i))) {
        return false;
      }
    }

    return true;
  }
}
