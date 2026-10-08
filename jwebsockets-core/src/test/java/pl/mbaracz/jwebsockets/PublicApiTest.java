package pl.mbaracz.jwebsockets;

import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.AccessFlag;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class PublicApiTest {

    private static final String API_PACKAGE = "pl.mbaracz.jwebsockets";

    @Test
    void publicApiShouldNotExposeNettyOrInaccessibleTypes() throws Exception {
        List<String> violations = new ArrayList<>();

        for (Class<?> apiType : findPublicApiTypes()) {
            inspectTypes(apiType, apiType.getTypeParameters(), violations);
            inspectType(apiType, apiType.getGenericSuperclass(), violations);
            for (Type interfaceType : apiType.getGenericInterfaces()) {
                inspectType(apiType, interfaceType, violations);
            }

            for (Constructor<?> constructor : apiType.getDeclaredConstructors()) {
                if (isApiMember(constructor.getModifiers())) {
                    inspectTypes(constructor, constructor.getTypeParameters(), violations);
                    inspectTypes(constructor, constructor.getGenericParameterTypes(), violations);
                    inspectTypes(constructor, constructor.getGenericExceptionTypes(), violations);
                }
            }
            for (Method method : apiType.getDeclaredMethods()) {
                if (isApiMember(method.getModifiers()) && !method.accessFlags().contains(AccessFlag.SYNTHETIC)) {
                    inspectTypes(method, method.getTypeParameters(), violations);
                    inspectType(method, method.getGenericReturnType(), violations);
                    inspectTypes(method, method.getGenericParameterTypes(), violations);
                    inspectTypes(method, method.getGenericExceptionTypes(), violations);
                }
            }
            for (Field field : apiType.getDeclaredFields()) {
                if (isApiMember(field.getModifiers()) && !field.accessFlags().contains(AccessFlag.SYNTHETIC)) {
                    inspectType(field, field.getGenericType(), violations);
                }
            }
        }

        assertThat(violations).isEmpty();
    }

    @Test
    void nettyImplementationTypesShouldRemainHidden() {
        assertThat(List.of(
            ClosingHandshake.class,
            CloseInfo.class,
            InMemoryTopicBroker.class,
            ReadGate.class,
            ReservedBitsValidator.class,
            SerialExecutor.class,
            SessionRegistry.class,
            WebSocketServerChannelInitializer.class,
            WebSocketServerHandler.class
        )).allMatch(type -> !Modifier.isPublic(type.getModifiers()));
    }

    @Test
    void topicBrokerShouldRemainPublicForExternalImplementations() {
        assertThat(TopicBroker.class).matches(type -> type.isInterface() && Modifier.isPublic(type.getModifiers()));
        assertThat(TopicMessageHandler.class)
            .matches(type -> type.isInterface() && Modifier.isPublic(type.getModifiers()));
    }

    @Test
    void shouldInspectGenericArrayComponentBounds() throws Exception {
        Type genericArray = NettyBound.class.getDeclaredMethod("values").getGenericReturnType();
        List<String> violations = new ArrayList<>();

        inspectType(NettyBound.class, genericArray, violations);

        assertThat(violations).singleElement().asString().contains(Channel.class.getName());
    }

    private static boolean isApiMember(int modifiers) {
        return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
    }

    private static List<Class<?>> findPublicApiTypes() throws Exception {
        Path classesRoot = Path.of(WebSocketServer.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        Path apiRoot = classesRoot.resolve(API_PACKAGE.replace('.', File.separatorChar));

        try (Stream<Path> classFiles = Files.walk(apiRoot)) {
            return classFiles
                .filter(path -> path.getFileName().toString().endsWith(".class"))
                .sorted()
                .<Class<?>>map(path -> loadClass(classesRoot, path))
                .filter(PublicApiTest::isPubliclyAccessible)
                .toList();
        }
    }

    private static Class<?> loadClass(Path classesRoot, Path classFile) {
        String className = classesRoot.relativize(classFile)
            .toString()
            .replace(File.separatorChar, '.')
            .replaceFirst("\\.class$", "");

        try {
            return Class.forName(className, false, PublicApiTest.class.getClassLoader());
        } catch (ClassNotFoundException exception) {
            throw new IllegalStateException("Could not load compiled class " + className, exception);
        }
    }

    private static boolean isPubliclyAccessible(Class<?> type) {
        for (Class<?> current = type; current != null; current = current.getEnclosingClass()) {
            if (!Modifier.isPublic(current.getModifiers())) {
                return false;
            }
        }
        return true;
    }

    private static void inspectTypes(Object owner, Type[] types, List<String> violations) {
        for (Type type : types) {
            inspectType(owner, type, violations);
        }
    }

    private static void inspectType(Object owner, Type type, List<String> violations) {
        inspectType(owner, type, violations, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static void inspectType(Object owner, Type type, List<String> violations, Set<Type> inspected) {
        if (type == null) {
            return;
        }
        if (!inspected.add(type)) {
            return;
        }
        if (type instanceof Class<?> referencedClass) {
            while (referencedClass.isArray()) {
                referencedClass = referencedClass.getComponentType();
            }
            String name = referencedClass.getName();
            if (name.startsWith("io.netty.")) {
                violations.add(owner + " exposes " + name);
            }
            if (name.startsWith(API_PACKAGE + ".") && !isPubliclyAccessible(referencedClass)) {
                violations.add(owner + " exposes inaccessible " + name);
            }
        } else if (type instanceof ParameterizedType parameterizedType) {
            inspectType(owner, parameterizedType.getOwnerType(), violations, inspected);
            inspectType(owner, parameterizedType.getRawType(), violations, inspected);
            inspectTypes(owner, parameterizedType.getActualTypeArguments(), violations, inspected);
        } else if (type instanceof WildcardType wildcardType) {
            inspectTypes(owner, wildcardType.getLowerBounds(), violations, inspected);
            inspectTypes(owner, wildcardType.getUpperBounds(), violations, inspected);
        } else if (type instanceof GenericArrayType genericArrayType) {
            inspectType(owner, genericArrayType.getGenericComponentType(), violations, inspected);
        } else if (type instanceof TypeVariable<?> typeVariable) {
            inspectTypes(owner, typeVariable.getBounds(), violations, inspected);
        }
    }

    private static void inspectTypes(Object owner, Type[] types, List<String> violations, Set<Type> inspected) {
        for (Type type : types) {
            inspectType(owner, type, violations, inspected);
        }
    }

    private abstract static class NettyBound<T extends Channel> {

        abstract T[] values();
    }
}
