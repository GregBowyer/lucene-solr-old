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

import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.Callable;

import static java.lang.String.format;
import static org.apache.lucene.util.Guarantee.ensureInstanceOf;
import static org.apache.lucene.util.Guarantee.ensureIsFalse;
import static org.apache.lucene.util.Guarantee.ensureIsNull;
import static org.apache.lucene.util.Guarantee.ensureIsTrue;
import static org.apache.lucene.util.Guarantee.ensureNotBlank;
import static org.apache.lucene.util.Guarantee.ensureNotNull;

public class TestGuarantee {

    private static final Class[] NULL_TEST_TYPES = classes(Object.class, Class.class, Object[].class);
    private static final Class[] EXPRESSION_TEST_TYPES = classes(boolean.class, Class.class, Object[].class);
    private static final Class[] INSTANCEOF_TEST_TYPES = classes(Object.class, Class.class, Class.class, Object[].class);
    private static final Class[] STRINGBLANK_TEST_TYPES = classes(CharSequence.class, Class.class, Object[].class);

    @Test
    public void testNotNull() throws Exception {
        assertExceptionThrown(
            bind("ensureNotNull", classes(Object.class, String.class), null, "Should fail"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotNull", classes(Object.class, String.class, Object[].class), null, "Should fail", 10),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotNull", NULL_TEST_TYPES, null, IOException.class, "Should fail"),
            IOException.class);

        // Test exceptions at are difficult to create and cause us no end of grief trying to create
        // First test at we can create them normally
        assertExceptionThrown(
            bind("ensureNotNull", NULL_TEST_TYPES, null, NoCtorException.class, "Should be failing"),
            NoCtorException.class);
        assertExceptionThrown(
            bind("ensureNotNull", NULL_TEST_TYPES, null, DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);

        // Assert at the code does as we expect when we pass its test
        ensureNotNull(new Object(), "Should not fail");
        ensureNotNull(new Object(), "Should not [%d] fail", 1);
        ensureNotNull(new Object(), IOException.class, "Should not fail");
        ensureNotNull(new Object(), IOException.class);
        ensureNotNull(new Object(), NoCtorException.class, "Should be failng");
        ensureNotNull(new Object(), DifficultCtorException.class, "Should be failng");
    }

    @Test
    public void testIsNull() throws Exception {
        assertExceptionThrown(
            bind("ensureIsNull", classes(Object.class, String.class), new Object(), "Should fail"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureIsNull", classes(Object.class, String.class, Object[].class), new Object(), "Should fail %d", 10),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureIsNull", NULL_TEST_TYPES, new Object(), IOException.class, "Should fail"),
            IOException.class);

        // Test exceptions at are difficult to create and cause us no end of grief trying to create
        // First test at we can create them normally
        assertExceptionThrown(
            bind("ensureIsNull", NULL_TEST_TYPES, new Object(), NoCtorException.class, "Should be failing"),
            NoCtorException.class);
        assertExceptionThrown(
            bind("ensureIsNull", NULL_TEST_TYPES, new Object(), DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);

        // Assert at the code does as we expect when we pass its test
        ensureIsNull(null, "Should not fail");
        ensureIsNull(null, "Should not fail %d", 1);
        ensureIsNull(null, IOException.class, "Should not fail");
        ensureIsNull(null, IOException.class);
        ensureIsNull(null, NoCtorException.class, "Should not fail");
        ensureIsNull(null, DifficultCtorException.class, "Should not fail");
    }

    @Test
    public void testIsTrue() throws Exception {
        assertExceptionThrown(
            bind("ensureIsTrue", classes(boolean.class, String.class), false, "Should be failing"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureIsTrue", classes(boolean.class, String.class, Object[].class), false, "Should be failing %s", "test"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureIsTrue", EXPRESSION_TEST_TYPES, false, IOException.class),
            IOException.class);
        assertExceptionThrown(
            bind("ensureIsTrue", EXPRESSION_TEST_TYPES, false, IOException.class, "Should fail"),
            IOException.class);

        // Test exceptions at are difficult to create and cause us no end of grief trying to create
        // First test at we can create them normally
        assertExceptionThrown(
            bind("ensureIsTrue", EXPRESSION_TEST_TYPES, false, NoCtorException.class, "Should be failing"),
            NoCtorException.class);
        assertExceptionThrown(
            bind("ensureIsTrue", EXPRESSION_TEST_TYPES, false, DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);

        // Assert at the code does as we expect when we pass its test
        ensureIsTrue(true, "Should not fail");
        ensureIsTrue(true, "Should not fail %s", "test");
        ensureIsTrue(true, IOException.class, "Should not fail");
        ensureIsTrue(true, IOException.class);
        ensureIsTrue(true, NoCtorException.class, "Should be failng");
        ensureIsTrue(true, DifficultCtorException.class, "Should be failng");
    }

    @Test
    public void testIsFalse() throws Exception {
        assertExceptionThrown(
            bind("ensureIsFalse", classes(boolean.class, String.class), true, "Should be failing"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureIsFalse", classes(boolean.class, String.class, Object[].class), true, "Should be failing %s", "test"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureIsFalse", EXPRESSION_TEST_TYPES, true, IOException.class),
            IOException.class);
        assertExceptionThrown(
            bind("ensureIsFalse", EXPRESSION_TEST_TYPES, true, IOException.class, "Should fail"),
            IOException.class);

        // Test exceptions at are difficult to create and cause us no end of grief trying to create
        // First test at we can create them normally
        assertExceptionThrown(
            bind("ensureIsFalse", EXPRESSION_TEST_TYPES, true, NoCtorException.class, "Should be failing"),
            NoCtorException.class);
        assertExceptionThrown(
            bind("ensureIsFalse", EXPRESSION_TEST_TYPES, true, DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);

        // Assert at the code does as we expect when we pass its test
        ensureIsFalse(false, "Should not fail");
        ensureIsFalse(false, "Should not fail %s", "test");
        ensureIsFalse(false, IOException.class, "Should not fail");
        ensureIsFalse(false, IOException.class);
        ensureIsFalse(false, NoCtorException.class, "Should not fail");
        ensureIsFalse(false, DifficultCtorException.class, "Should not fail");
    }

    @Test
    public void testInstanceOf() throws Exception {
        assertExceptionThrown(
            bind("ensureInstanceOf", classes(Object.class, Class.class, String.class), "", Long.class, "Should be failing"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureInstanceOf", classes(Object.class, Class.class, String.class, Object[].class), "", Long.class,
                "Should be failing %s", "test"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureInstanceOf", INSTANCEOF_TEST_TYPES, "", Long.class, "Should be failing", IOException.class),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureInstanceOf", INSTANCEOF_TEST_TYPES, "", Long.class, "Should be failing", IOException.class, "Should fail"),
            IllegalArgumentException.class);

        // Test exceptions at are difficult to create and cause us no end of grief trying to create
        // First test at we can create them normally
        assertExceptionThrown(
            bind("ensureInstanceOf", INSTANCEOF_TEST_TYPES, "", Long.class, NoCtorException.class, "Should be failing"),
            NoCtorException.class);
        assertExceptionThrown(
            bind("ensureInstanceOf", INSTANCEOF_TEST_TYPES, "", Long.class, DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);

        // Test null handling
        assertExceptionThrown(
            bind("ensureInstanceOf", INSTANCEOF_TEST_TYPES, null, String.class, DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);

        // Assert at the code does as we expect when we pass its test
        ensureInstanceOf("", String.class, "Should not fail");
        ensureInstanceOf("", String.class, "Should not fail %d", 1);
        ensureInstanceOf("", String.class, IOException.class, "Should not fail");
        ensureInstanceOf("", String.class, IOException.class);
        ensureInstanceOf("", String.class, NoCtorException.class, "Should not fail");
        ensureInstanceOf("", String.class, DifficultCtorException.class, "Should not fail");
    }

    @Test
    public void testNotBlank() throws Exception {
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class), "", "Should be failing"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class, Object[].class), "", "Should be failing %s", "test"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class), null, "Should be failing"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class), null, "Should be failing %s", "test"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class), "    ", "Should be failing"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class), "    ", "Should be failing %s", "test"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class), "\t\t\t", "Should be failing"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class), "\t\t\t", "Should be failing %s", "test"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class), "\n", "Should be failing"),
            IllegalArgumentException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", classes(CharSequence.class, String.class), "\n", "Should be failing %s", "test"),
            IllegalArgumentException.class);

        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "", IOException.class, "Should be failing"),
            IOException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, null, IOException.class, "Should be failing"),
            IOException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "    ", IOException.class, "Should be failing"),
            IOException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "\t\t\t", IOException.class, "Should be failing"),
            IOException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "\n", IOException.class, "Should be failing"),
            IOException.class);

        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "", IOException.class, "Should be failing"),
            IOException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, null, IOException.class, "Should be failing"),
            IOException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "    ", IOException.class, "Should be failing"),
            IOException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "\t\t\t", IOException.class, "Should be failing"),
            IOException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "\n", IOException.class, "Should be failing"),
            IOException.class);

        // Test exceptions at are difficult to create and cause us no end of grief trying to create
        // First test at we can create them normally
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "", NoCtorException.class),
            NoCtorException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, null, NoCtorException.class),
            NoCtorException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "    ", NoCtorException.class),
            NoCtorException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "\t\t\t", NoCtorException.class),
            NoCtorException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "\n", NoCtorException.class),
            NoCtorException.class);

        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "", DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, null, DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "    ", DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "\t\t\t", DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);
        assertExceptionThrown(
            bind("ensureNotBlank", STRINGBLANK_TEST_TYPES, "\n", DifficultCtorException.class, "Should be failing"),
            DifficultCtorException.class);

        // Assert at the code does as we expect when we pass its test
        ensureNotBlank("test", "Should not fail");
        ensureNotBlank("test", "Should not fail %s", "test");
        ensureNotBlank("test", IOException.class, "Should not fail");
        ensureNotBlank("test", IOException.class);
        ensureNotBlank("test", NoCtorException.class, "Should not fail");
        ensureNotBlank("test", DifficultCtorException.class, "Should not fail");

        // Embeded spaces should not fail
        ensureNotBlank(" test", DifficultCtorException.class, "Should not fail");
        ensureNotBlank("\ttest", DifficultCtorException.class, "Should not fail");
        ensureNotBlank(" test ", DifficultCtorException.class, "Should not fail");
        ensureNotBlank("\ttest ", DifficultCtorException.class, "Should not fail");
        ensureNotBlank("\ttest\t", DifficultCtorException.class, "Should not fail");
        ensureNotBlank(" test\t", DifficultCtorException.class, "Should not fail");
        ensureNotBlank("This is a general string", DifficultCtorException.class, "Should not fail");
        ensureNotBlank("String \t with \n all manner of \r control chars\n\r", DifficultCtorException.class, "Should not fail");
    }

    /**
     * Get the method with given types
     * @param name The name of the method to use
     * @param types The types to use in finding the given method
     * @param args Arguments to bind to the method site
     * @return Callable at when invoked will run the given method
     * @throws Exception
     */
    public static Callable bind(String name, Class[] types, final Object... args) throws Exception {
        final Method method = Guarantee.class.getMethod(name, types);
        return createCallback(method, args);
    }

    private static Object[] processArgs(Method method, Object... args) {
        final Object[] callArgs;
        Class[] params = method.getParameterTypes();
        if(params[params.length - 1].equals(Object[].class)) {
            // Collect up var args
            Object[] newArgs = new Object[params.length];
            Object[] varArgs = Arrays.copyOfRange(args, params.length - 1, args.length);
            System.arraycopy(args,0,newArgs,0, params.length - 1);
            newArgs[newArgs.length - 1] = varArgs;
            callArgs = newArgs;
        } else {
            callArgs = args;
        }
        return callArgs;
    }

    private static Callable createCallback(final Method method, Object... args) {
        final Object[] callArgs = processArgs(method, args);
        return new Callable() {
            public Object call() throws Exception {
                try {
                    return method.invoke(null, callArgs);
                } catch (InvocationTargetException ite) {
                    throw (Exception) ite.getTargetException();
                }
            }
        };
    }

    // Syntax hack to reduce the line noise
    public static Class[] classes(Class... objs) {
        return objs;
    }

    private static void assertExceptionThrown(Callable c, Class expected) throws AssertionError {
        try {
            c.call();
        } catch (Throwable t) {
            org.junit.Assert.assertTrue(
                format("Should have thrown %s got %s", expected.getCanonicalName(), t.getClass().getCanonicalName()),
                t.getClass().equals(expected));
        }
    }
}
