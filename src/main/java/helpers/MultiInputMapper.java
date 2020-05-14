package helpers;

import java.io.InvalidClassException;

public interface MultiInputMapper<K, V, T> {
    T parseInput(K key, V value) throws InvalidClassException;
}
