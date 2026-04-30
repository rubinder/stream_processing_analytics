package com.example.common;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface SerializableKeyExtractor<T> extends Function<T, byte[]>, Serializable {}
