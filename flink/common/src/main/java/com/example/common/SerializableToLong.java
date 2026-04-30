package com.example.common;

import java.io.Serializable;
import java.util.function.ToLongFunction;

@FunctionalInterface
public interface SerializableToLong<T> extends ToLongFunction<T>, Serializable {}
